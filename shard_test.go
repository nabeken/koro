package koro

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	records1 = []*dynamodbstreams.Record{
		{
			EventID: aws.String("event-1"),
			Dynamodb: &dynamodbstreams.StreamRecord{
				SequenceNumber: aws.String("0000"),
			},
		},
		{
			EventID: aws.String("event-2"),
			Dynamodb: &dynamodbstreams.StreamRecord{
				SequenceNumber: aws.String("0001"),
			},
		},
	}

	records2 = []*dynamodbstreams.Record{
		{
			EventID: aws.String("event-3"),
			Dynamodb: &dynamodbstreams.StreamRecord{
				SequenceNumber: aws.String("0002"),
			},
		},
		{
			EventID: aws.String("event-4"),
			Dynamodb: &dynamodbstreams.StreamRecord{
				SequenceNumber: aws.String("0003"),
			},
		},
	}

	records3 = []*dynamodbstreams.Record{
		{
			EventID: aws.String("event-5"),
			Dynamodb: &dynamodbstreams.StreamRecord{
				SequenceNumber: aws.String("0004"),
			},
		},
		{
			EventID: aws.String("event-6"),
			Dynamodb: &dynamodbstreams.StreamRecord{
				SequenceNumber: aws.String("0005"),
			},
		},
	}
)

func TestSortShard(t *testing.T) {
	assert := assert.New(t)

	given := []*dynamodbstreams.Shard{
		{
			ShardId: aws.String("root"),
		},
		{
			ParentShardId: aws.String("leaf-1"),
			ShardId:       aws.String("leaf-2"),
		},
		{
			ParentShardId: aws.String("root"),
			ShardId:       aws.String("leaf-1"),
		},
		{
			ParentShardId: aws.String("leaf-2"),
			ShardId:       aws.String("leaf-3"),
		},
	}

	expected := []*dynamodbstreams.Shard{
		{
			ShardId: aws.String("root"),
		},
		{
			ParentShardId: aws.String("root"),
			ShardId:       aws.String("leaf-1"),
		},
		{
			ParentShardId: aws.String("leaf-1"),
			ShardId:       aws.String("leaf-2"),
		},
		{
			ParentShardId: aws.String("leaf-2"),
			ShardId:       aws.String("leaf-3"),
		},
	}

	assert.Equal(expected, SortShards(given))
}

func TestShardReader(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	ctrl := gomock.NewController(t)

	client := NewMockDynamoDBStreamer(ctrl)
	srs := NewShardReaderService(aws.String("test-arn"), client)

	shard := &dynamodbstreams.Shard{
		SequenceNumberRange: &dynamodbstreams.SequenceNumberRange{
			StartingSequenceNumber: aws.String("0000"),
			EndingSequenceNumber:   aws.String("0003"),
		},
		ShardId: aws.String("shard-1"),
	}

	sr := srs.NewReader(shard)

	require.True(sr.Next())
	assert.Equal("shard-1", sr.ShardID())

	var expects []*gomock.Call

	// should request an iterator for the latest record
	expects = append(expects, client.EXPECT().GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
		ShardId:           aws.String("shard-1"),
		ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeTrimHorizon),
		StreamArn:         aws.String("test-arn"),
	}).Return(&dynamodbstreams.GetShardIteratorOutput{
		ShardIterator: aws.String("itr-1"),
	}, nil))

	expects = append(expects, client.EXPECT().GetRecords(&dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String("itr-1"),
	}).Return(&dynamodbstreams.GetRecordsOutput{
		NextShardIterator: aws.String("itr-2"),
		Records:           records1,
	}, nil))

	actual, err := sr.ReadRecords()
	assert.NoError(err)
	assert.ElementsMatch(records1, actual)
	require.True(sr.Next())

	expects = append(expects, client.EXPECT().GetRecords(&dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String("itr-2"),
	}).Return(&dynamodbstreams.GetRecordsOutput{
		Records: records2,
	}, nil))

	actual, err = sr.ReadRecords()
	assert.NoError(err)
	assert.ElementsMatch(records2, actual)
	require.False(sr.Next(), "because the shard is closed")

	actual, err = sr.ReadRecords()
	assert.ErrorIs(err, ErrEndOfShard)
	assert.Nil(actual)

	// seek to the beginning
	sr.Seek(records1[0])

	// should seek to 0000
	expects = append(expects, client.EXPECT().GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
		SequenceNumber:    aws.String("0000"),
		ShardId:           aws.String("shard-1"),
		ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeAtSequenceNumber),
		StreamArn:         aws.String("test-arn"),
	}).Return(&dynamodbstreams.GetShardIteratorOutput{
		ShardIterator: aws.String("itr-5"),
	}, nil))

	expects = append(expects, client.EXPECT().GetRecords(&dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String("itr-5"),
	}).Return(&dynamodbstreams.GetRecordsOutput{
		Records: records1,
	}, nil))

	actual, err = sr.ReadRecords()
	assert.NoError(err)
	assert.ElementsMatch(records1, actual)
	require.False(sr.Next(), "because the shard is closed")

	gomock.InOrder(expects...)
}
