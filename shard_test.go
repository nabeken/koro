package koro

import (
	context "context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var (
	records1 = []types.Record{
		{
			EventID: aws.String("event-1"),
			Dynamodb: &types.StreamRecord{
				SequenceNumber: aws.String("0000"),
			},
		},
		{
			EventID: aws.String("event-2"),
			Dynamodb: &types.StreamRecord{
				SequenceNumber: aws.String("0001"),
			},
		},
	}

	records2 = []types.Record{
		{
			EventID: aws.String("event-3"),
			Dynamodb: &types.StreamRecord{
				SequenceNumber: aws.String("0002"),
			},
		},
		{
			EventID: aws.String("event-4"),
			Dynamodb: &types.StreamRecord{
				SequenceNumber: aws.String("0003"),
			},
		},
	}

	records3 = []types.Record{
		{
			EventID: aws.String("event-5"),
			Dynamodb: &types.StreamRecord{
				SequenceNumber: aws.String("0004"),
			},
		},
		{
			EventID: aws.String("event-6"),
			Dynamodb: &types.StreamRecord{
				SequenceNumber: aws.String("0005"),
			},
		},
	}
)

func TestSortShard(t *testing.T) {
	tcs := []struct {
		Description string
		Given       []types.Shard
		Expected    []types.Shard
	}{
		{
			Description: "With Root",
			Given: []types.Shard{
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
			},

			Expected: []types.Shard{
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
			},
		},
		{
			Description: "Without Root",
			Given: []types.Shard{
				{
					ParentShardId: aws.String("root-but-no-longer-exists"),
					ShardId:       aws.String("top"),
				},
				{
					ParentShardId: aws.String("leaf-1"),
					ShardId:       aws.String("leaf-2"),
				},
				{
					ParentShardId: aws.String("top"),
					ShardId:       aws.String("leaf-1"),
				},
				{
					ParentShardId: aws.String("leaf-2"),
					ShardId:       aws.String("leaf-3"),
				},
			},

			Expected: []types.Shard{
				{
					ParentShardId: aws.String("root-but-no-longer-exists"),
					ShardId:       aws.String("top"),
				},
				{
					ParentShardId: aws.String("top"),
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
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.Description, func(t *testing.T) {
			assert := assert.New(t)
			assert.Equal(tc.Expected, SortShards(tc.Given))
		})
	}
}

func TestShardReader(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	ctrl := gomock.NewController(t)

	client := NewMockDynamoDBStreamer(ctrl)
	srs := NewShardReaderService(aws.String("test-arn"), client)

	shard := &types.Shard{
		SequenceNumberRange: &types.SequenceNumberRange{
			StartingSequenceNumber: aws.String("0000"),
			EndingSequenceNumber:   aws.String("0003"),
		},
		ShardId: aws.String("shard-1"),
	}

	sr := srs.NewReader(shard)

	require.True(sr.Next())
	assert.Equal("shard-1", sr.ShardID())

	var expects []any

	// should request an iterator for the latest record
	expects = append(expects, client.EXPECT().GetShardIterator(gomock.Any(), &dynamodbstreams.GetShardIteratorInput{
		ShardId:           aws.String("shard-1"),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
		StreamArn:         aws.String("test-arn"),
	}).Return(&dynamodbstreams.GetShardIteratorOutput{
		ShardIterator: aws.String("itr-1"),
	}, nil))

	expects = append(expects, client.EXPECT().GetRecords(gomock.Any(), &dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String("itr-1"),
	}).Return(&dynamodbstreams.GetRecordsOutput{
		NextShardIterator: aws.String("itr-2"),
		Records:           records1,
	}, nil))

	actual, err := sr.ReadRecords(context.Background())
	assert.NoError(err)
	assert.ElementsMatch(records1, actual)
	require.True(sr.Next())

	expects = append(expects, client.EXPECT().GetRecords(gomock.Any(), &dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String("itr-2"),
	}).Return(&dynamodbstreams.GetRecordsOutput{
		Records: records2,
	}, nil))

	actual, err = sr.ReadRecords(context.Background())
	assert.NoError(err)
	assert.ElementsMatch(records2, actual)
	require.False(sr.Next(), "because the shard is closed")

	actual, err = sr.ReadRecords(context.Background())
	assert.ErrorIs(err, ErrEndOfShard)
	assert.Nil(actual)

	// seek to the beginning
	sr.Seek(&records1[0])

	// should seek to 0000
	expects = append(expects, client.EXPECT().GetShardIterator(gomock.Any(), &dynamodbstreams.GetShardIteratorInput{
		SequenceNumber:    aws.String("0000"),
		ShardId:           aws.String("shard-1"),
		ShardIteratorType: types.ShardIteratorTypeAtSequenceNumber,
		StreamArn:         aws.String("test-arn"),
	}).Return(&dynamodbstreams.GetShardIteratorOutput{
		ShardIterator: aws.String("itr-5"),
	}, nil))

	expects = append(expects, client.EXPECT().GetRecords(gomock.Any(), &dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String("itr-5"),
	}).Return(&dynamodbstreams.GetRecordsOutput{
		Records: records1,
	}, nil))

	actual, err = sr.ReadRecords(context.Background())
	assert.NoError(err)
	assert.ElementsMatch(records1, actual)
	require.False(sr.Next(), "because the shard is closed")

	gomock.InOrder(expects...)
}
