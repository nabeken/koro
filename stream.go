//go:generate mockgen -source=$GOFILE -destination=mock_$GOFILE -package=$GOPACKAGE
package koro

import (
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

// DynamoDBStreamer is a subset of DynamoDB Streams API interface.
type DynamoDBStreamer interface {
	GetRecords(*dynamodbstreams.GetRecordsInput) (*dynamodbstreams.GetRecordsOutput, error)
	GetShardIterator(*dynamodbstreams.GetShardIteratorInput) (*dynamodbstreams.GetShardIteratorOutput, error)
	ListStreams(*dynamodbstreams.ListStreamsInput) (*dynamodbstreams.ListStreamsOutput, error)
	DescribeStream(*dynamodbstreams.DescribeStreamInput) (*dynamodbstreams.DescribeStreamOutput, error)
}

// ErrNoAvailShards will be returned when there are no available shards for reading.
// When the reader reads a last record in a last shard in a disabled stream, it will be returned.
var ErrNoAvailShards = errors.New("koro: no available shards to read")

// StreamReader reads shards in serial order. It does maintain a checkpoint in-memory.
type StreamReader struct {
	client *client

	arn *string
	srs *ShardReaderService

	mu          sync.Mutex
	readers     []*ShardReader
	por         int
	lastShardId string
}

// NewStreamByName creates a *StreamReader by a given table name.
func NewStreamByName(dsr DynamoDBStreamer, tn string) (*StreamReader, error) {
	client := &client{client: dsr}

	arn, err := client.describeStreamArn(tn)
	if err != nil {
		return nil, err
	}

	return newStreamReader(client, arn)
}

// NewStreamByName creates a *StreamReader by a stream ARN.
func NewStreamReader(dsr DynamoDBStreamer, arn *string) (*StreamReader, error) {
	return newStreamReader(&client{client: dsr}, arn)
}

func newStreamReader(client *client, arn *string) (*StreamReader, error) {
	sr := &StreamReader{
		client: client,
		arn:    arn,
		srs:    NewShardReaderService(arn, client.client),
	}

	if err := sr.init(); err != nil {
		return nil, fmt.Errorf("initializing the reader: %w", err)
	}

	return sr, nil
}

func (sr *StreamReader) init() error {
	shards, err := sr.client.describeShards(sr.arn)
	if err != nil {
		return err
	}

	// this guard ensures that len(shards) never be 0
	if len(shards) == 0 {
		return ErrNoAvailShards
	}

	var readers []*ShardReader
	for _, s := range shards {
		readers = append(readers, sr.srs.NewReader(s))
	}

	sr.readers = readers

	return nil
}

// reader returns a shard reader that is currently read. A caller must hold the lock.
func (sr *StreamReader) reader() *ShardReader {
	return sr.readers[sr.por]
}

// Reader returns a shard reader that is currently read.
func (sr *StreamReader) Reader() *ShardReader {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	return sr.reader()
}

func (sr *StreamReader) moveToNextReader() error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.lastShardId = sr.reader().ShardID()

	sr.por++
	if sr.por < len(sr.readers) {
		return nil
	}

	// seems like the reader reaches the end of shards we know
	// let's refresh shards information
	if err := sr.init(); err != nil {
		return fmt.Errorf("refreshing the reader: %w", err)
	}

	sr.seekReader()

	// if the last shard is a shard we have read, there are no new open shard. Probably, the stream has been disabled.
	if sr.por+1 == len(sr.readers) {
		return ErrNoAvailShards
	}

	// there are new shards for reading
	sr.por++

	return nil
}

// seekReader advances the current reader of the lastShardId.
func (sr *StreamReader) seekReader() {
	for i := range sr.readers {
		if sr.lastShardId == sr.readers[i].ShardID() {
			sr.por = i
			break
		}
	}
}

// ReadRecords reads the current shard. It will only move to the next shard if the current shard reader reaches the end of shard.
func (sr *StreamReader) ReadRecords() ([]*dynamodbstreams.Record, error) {
	for {
		r := sr.Reader()

		if r.Next() {
			return r.ReadRecords()
		}

		// move on the next shard
		if err := sr.moveToNextReader(); err != nil {
			return nil, err
		}
	}
}

// Seek advances the iterator in the current shard. See ShardReader.Seek.
// This should be called right after ReadRecords() if you want to restart from the same shard.
func (sr *StreamReader) Seek(rc *dynamodbstreams.Record) {
	sr.Reader().Seek(rc)
}

type client struct {
	client DynamoDBStreamer
}

func (c *client) describeStreamArn(tn string) (*string, error) {
	req := &dynamodbstreams.ListStreamsInput{
		TableName: aws.String(tn),
	}
	for {
		resp, err := c.client.ListStreams(req)
		if err != nil {
			return nil, err
		}

		for _, s := range resp.Streams {
			if tn == *s.TableName {
				return s.StreamArn, nil
			}
		}

		if resp.LastEvaluatedStreamArn == nil {
			break
		}

		req.ExclusiveStartStreamArn = resp.LastEvaluatedStreamArn
	}

	return nil, fmt.Errorf("no stream found for %s", tn)
}

func (c *client) describeShards(arn *string) ([]*dynamodbstreams.Shard, error) {
	req := &dynamodbstreams.DescribeStreamInput{
		StreamArn: arn,
	}

	var shards []*dynamodbstreams.Shard
	for {
		resp, err := c.client.DescribeStream(req)
		if err != nil {
			return nil, err
		}

		shards = append(shards, resp.StreamDescription.Shards...)
		if resp.StreamDescription.LastEvaluatedShardId == nil {
			break
		}

		req.ExclusiveStartShardId = resp.StreamDescription.LastEvaluatedShardId
	}

	return SortShards(shards), nil
}
