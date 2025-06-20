package koro

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

var (
	ErrEndOfShard = errors.New("koro: End of Shard")
)

// ShardReaderService is a factory service that creates a new *ShardReader from *types.Shard.
type ShardReaderService struct {
	arn    *string
	client DynamoDBStreamer
}

// NewShardReaderService creates a *ShardReaderService.
func NewShardReaderService(arn *string, client DynamoDBStreamer) *ShardReaderService {
	return &ShardReaderService{
		arn:    arn,
		client: client,
	}
}

// NewReader creates a *ShardReader by *types.Shard.
func (srs *ShardReaderService) NewReader(shard *types.Shard) *ShardReader {
	return &ShardReader{
		client:    srs.client,
		streamArn: srs.arn,
		shard:     shard,
	}
}

// ShardReader provides a reader interface for *types.Shard.
type ShardReader struct {
	client    DynamoDBStreamer
	streamArn *string
	shard     *types.Shard

	rpos *string
	itr  *string

	// true if the reader at the end of shard
	eos bool
}

func IsShardNotFoundError(origErr error) bool {
	var errResourceNotFound *types.ResourceNotFoundException

	return errors.As(origErr, &errResourceNotFound)
}

// Next returns true if the reader doesn't reach the end of shard.
func (r *ShardReader) Next() bool {
	return !r.eos
}

func (r *ShardReader) ShardID() string {
	return *r.shard.ShardId
}

// ReadRecords reads records from the shard. It will automatically update the shard iterator for you.
func (r *ShardReader) ReadRecords(ctx context.Context) ([]types.Record, error) {
	if r.eos {
		return nil, ErrEndOfShard
	}

	itr, err := r.getShardIterator(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := r.client.GetRecords(
		ctx,
		&dynamodbstreams.GetRecordsInput{
			ShardIterator: itr,
		},
	)

	if err != nil {
		if IsShardNotFoundError(err) {
			r.eos = true
			return nil, nil
		}
		return nil, fmt.Errorf("getting the records: %w", err)
	}

	if nextItr := resp.NextShardIterator; nextItr != nil {
		r.itr = nextItr
		r.rpos = nil
	} else {
		r.eos = true
	}

	return resp.Records, nil
}

// Seek advances the iterator to a given record. The next iterator will read record at rc.
// When a caller is unable to a record, you should seek the iterator to the record in order to restart the processing at the record.
func (r *ShardReader) Seek(rc *types.Record) {
	r.rpos = rc.Dynamodb.SequenceNumber
	r.eos = false
	r.itr = nil
}

// Reset resets the internal state in order to read the shard from the beginning.
func (r *ShardReader) Reset() {
	r.rpos = nil
	r.eos = false
	r.itr = nil
}

func (r *ShardReader) getShardIterator(ctx context.Context) (*string, error) {
	if r.itr == nil {
		resp, err := r.client.GetShardIterator(ctx, r.buildShardIteratorRequest())
		if err != nil {
			return nil, fmt.Errorf("getting the shard iterator: %w", err)
		}
		return resp.ShardIterator, nil
	}

	return r.itr, nil
}

func (r *ShardReader) buildShardIteratorRequest() *dynamodbstreams.GetShardIteratorInput {
	if r.rpos == nil {
		// will read from the oldest
		return &dynamodbstreams.GetShardIteratorInput{
			ShardId:           r.shard.ShardId,
			ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
			StreamArn:         r.streamArn,
		}
	}

	// will request an iterator that starts at rpos
	return &dynamodbstreams.GetShardIteratorInput{
		ShardId:           r.shard.ShardId,
		ShardIteratorType: types.ShardIteratorTypeAtSequenceNumber,
		StreamArn:         r.streamArn,
		SequenceNumber:    r.rpos,
	}
}

// SortShards sorts shards from a parent-to-child.
func SortShards(shards []types.Shard) []types.Shard {
	newShards := make([]types.Shard, 0, len(shards))

	var prev string
	for i, s := range shards {
		if s.ParentShardId == nil || i == 0 { // root or first shard
			newShards = append(newShards, s)
			prev = *s.ShardId
			continue
		}

		// look up
		for _, ss := range shards {
			// skip root shard
			if ss.ParentShardId == nil {
				continue
			}

			// when we have already read the parent
			if *ss.ParentShardId == prev {
				newShards = append(newShards, ss)
				prev = *ss.ShardId
				break
			}
		}
	}

	return newShards
}
