package koro

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

type StreamReader struct {
	client *client

	arn *string
	srs *ShardReaderService

	readers []*ShardReader

	por int
}

func NewStreamByName(dsr DynamoDBStreamer, tn string) (*StreamReader, error) {
	client := &client{client: dsr}

	arn, err := client.describeStreamArn(tn)
	if err != nil {
		return nil, err
	}

	return newStreamReader(client, arn)
}

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
		return nil, err
	}

	return sr, nil
}

func (sr *StreamReader) init() error {
	shards, err := sr.client.describeShards(sr.arn)
	if err != nil {
		return err
	}

	var readers []*ShardReader
	for _, s := range shards {
		readers = append(readers, sr.srs.NewReader(s))
	}

	sr.readers = readers

	return nil
}

// Reader returns a shard reader that is currently read.
func (sr *StreamReader) Reader() *ShardReader {
	return sr.readers[sr.por]
}

func (sr *StreamReader) moveToNextReader() {
	sr.por++
	if sr.por > len(sr.readers) {
		// TODO: need to refresh readers to get new open shard
		sr.por = 0
		panic("FIXME")
	}
}

func (sr *StreamReader) ReadRecords() ([]*dynamodbstreams.Record, error) {
	for {
		r := sr.Reader()
		if r.Next() {
			return r.ReadRecords()
		}

		// move on the next shard
		sr.moveToNextReader()
	}
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
