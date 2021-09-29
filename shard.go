//go:generate mockgen -source=$GOFILE -destination=mock_$GOFILE -package=$GOPACKAGE
package koro

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

var (
	ErrEndOfShard = errors.New("koro: End of Shard")
)

type DynamoDBStreamer interface {
	GetRecords(*dynamodbstreams.GetRecordsInput) (*dynamodbstreams.GetRecordsOutput, error)
	GetShardIterator(*dynamodbstreams.GetShardIteratorInput) (*dynamodbstreams.GetShardIteratorOutput, error)
	ListStreams(*dynamodbstreams.ListStreamsInput) (*dynamodbstreams.ListStreamsOutput, error)
	DescribeStream(*dynamodbstreams.DescribeStreamInput) (*dynamodbstreams.DescribeStreamOutput, error)
}

type ShardReaderService struct {
	arn    *string
	client DynamoDBStreamer
}

func NewShardReaderService(arn *string, client DynamoDBStreamer) *ShardReaderService {
	return &ShardReaderService{
		arn:    arn,
		client: client,
	}
}

func (srs *ShardReaderService) NewReader(shard *dynamodbstreams.Shard) *ShardReader {
	return &ShardReader{
		client:    srs.client,
		streamArn: srs.arn,
		shard:     shard,
	}
}

type ShardReader struct {
	client    DynamoDBStreamer
	streamArn *string
	shard     *dynamodbstreams.Shard

	rpos *string
	itr  *string

	// true if the reader at the end of shard
	eos bool
}

func IsShardNotFoundError(origErr error) bool {
	err := errors.Unwrap(origErr)
	if err == nil {
		err = origErr
	}

	awsErr, ok := origErr.(awserr.Error)
	if !ok {
		return false
	}

	return awsErr.Code() == dynamodbstreams.ErrCodeResourceNotFoundException
}

func (r *ShardReader) Next() bool {
	return !r.eos
}

func (r *ShardReader) ShardID() string {
	return *r.shard.ShardId
}

func (r *ShardReader) ReadRecords() ([]*dynamodbstreams.Record, error) {
	if r.eos {
		return nil, ErrEndOfShard
	}

	itr, err := r.getShardIterator()
	if err != nil {
		return nil, err
	}

	resp, err := r.client.GetRecords(&dynamodbstreams.GetRecordsInput{
		ShardIterator: itr,
	})

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
func (r *ShardReader) Seek(rc *dynamodbstreams.Record) {
	r.rpos = rc.Dynamodb.SequenceNumber
	r.eos = false
	r.itr = nil
}

func (r *ShardReader) getShardIterator() (*string, error) {
	if r.itr == nil {
		resp, err := r.client.GetShardIterator(r.buildShardIteratorRequest())
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
			ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeTrimHorizon),
			StreamArn:         r.streamArn,
		}
	}

	// will request an iterator that starts at rpos
	return &dynamodbstreams.GetShardIteratorInput{
		ShardId:           r.shard.ShardId,
		ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeAtSequenceNumber),
		StreamArn:         r.streamArn,
		SequenceNumber:    r.rpos,
	}
}

func SortShards(shards []*dynamodbstreams.Shard) []*dynamodbstreams.Shard {
	newShards := make([]*dynamodbstreams.Shard, 0, len(shards))

	var prev string
	for _, s := range shards {
		if s.ParentShardId == nil { // root shard
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
