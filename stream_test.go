package koro

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	dynamodbstreams "github.com/aws/aws-sdk-go/service/dynamodbstreams"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestStreamReader(t *testing.T) {
	//assert := assert.New(t)
	require := require.New(t)

	ctrl := gomock.NewController(t)
	client := NewMockDynamoDBStreamer(ctrl)
	shards := []*dynamodbstreams.Shard{
		{
			SequenceNumberRange: &dynamodbstreams.SequenceNumberRange{
				StartingSequenceNumber: aws.String("0000"),
				EndingSequenceNumber:   aws.String("0001"),
			},
			ShardId: aws.String("shard-1"),
		},
		{
			ParentShardId: aws.String("shard-1"),
			SequenceNumberRange: &dynamodbstreams.SequenceNumberRange{
				StartingSequenceNumber: aws.String("0002"),
				EndingSequenceNumber:   aws.String("0003"),
			},
			ShardId: aws.String("shard-2"),
		},
	}

	var expects []*gomock.Call

	// should request an iterator for the latest record
	expects = append(expects, client.EXPECT().DescribeStream(&dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String("test-arn"),
	}).Return(&dynamodbstreams.DescribeStreamOutput{
		StreamDescription: &dynamodbstreams.StreamDescription{
			Shards:    shards,
			StreamArn: aws.String("test-arn"),
		},
	}, nil))

	sr, err := NewStreamReader(client, aws.String("test-arn"))
	require.NoError(err)

	// reading shard-1
	{
		expects = append(expects, client.EXPECT().GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
			ShardId:           aws.String("shard-1"),
			ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeTrimHorizon),
			StreamArn:         aws.String("test-arn"),
		}).Return(&dynamodbstreams.GetShardIteratorOutput{
			ShardIterator: aws.String("itr-1-1"),
		}, nil))

		expects = append(expects, client.EXPECT().GetRecords(&dynamodbstreams.GetRecordsInput{
			ShardIterator: aws.String("itr-1-1"),
		}).Return(&dynamodbstreams.GetRecordsOutput{
			Records:           records1,
			NextShardIterator: aws.String("itr-1-2"),
		}, nil))

		actual1, err1 := sr.ReadRecords()
		require.NoError(err1)
		require.ElementsMatch(records1, actual1)

		expects = append(expects, client.EXPECT().GetRecords(&dynamodbstreams.GetRecordsInput{
			ShardIterator: aws.String("itr-1-2"),
		}).Return(&dynamodbstreams.GetRecordsOutput{
			Records: records2,
		}, nil))
		actual2, err2 := sr.ReadRecords()
		require.NoError(err2)
		require.ElementsMatch(records2, actual2)
	}

	// reading shard-2
	{
		expects = append(expects, client.EXPECT().GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
			ShardId:           aws.String("shard-2"),
			ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeTrimHorizon),
			StreamArn:         aws.String("test-arn"),
		}).Return(&dynamodbstreams.GetShardIteratorOutput{
			ShardIterator: aws.String("itr-2-1"),
		}, nil))

		expects = append(expects, client.EXPECT().GetRecords(&dynamodbstreams.GetRecordsInput{
			ShardIterator: aws.String("itr-2-1"),
		}).Return(&dynamodbstreams.GetRecordsOutput{
			Records:           records1,
			NextShardIterator: aws.String("itr-2-2"),
		}, nil))

		actual1, err1 := sr.ReadRecords()
		require.NoError(err1)
		require.ElementsMatch(records1, actual1)

		expects = append(expects, client.EXPECT().GetRecords(&dynamodbstreams.GetRecordsInput{
			ShardIterator: aws.String("itr-2-2"),
		}).Return(&dynamodbstreams.GetRecordsOutput{
			Records: records2,
		}, nil))
		actual2, err2 := sr.ReadRecords()
		require.NoError(err2)
		require.ElementsMatch(records2, actual2)
	}

	// reading shard-3 that was added after the first attempt
	{
		shards = append(shards, &dynamodbstreams.Shard{
			ParentShardId: aws.String("shard-2"),
			SequenceNumberRange: &dynamodbstreams.SequenceNumberRange{
				StartingSequenceNumber: aws.String("0004"),
				EndingSequenceNumber:   aws.String("0005"),
			},
			ShardId: aws.String("shard-3"),
		})

		// should request an iterator for the latest record
		expects = append(expects, client.EXPECT().DescribeStream(&dynamodbstreams.DescribeStreamInput{
			StreamArn: aws.String("test-arn"),
		}).Return(&dynamodbstreams.DescribeStreamOutput{
			StreamDescription: &dynamodbstreams.StreamDescription{
				Shards:    shards,
				StreamArn: aws.String("test-arn"),
			},
		}, nil))

		expects = append(expects, client.EXPECT().GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
			ShardId:           aws.String("shard-3"),
			ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeTrimHorizon),
			StreamArn:         aws.String("test-arn"),
		}).Return(&dynamodbstreams.GetShardIteratorOutput{
			ShardIterator: aws.String("itr-3-1"),
		}, nil))

		expects = append(expects, client.EXPECT().GetRecords(&dynamodbstreams.GetRecordsInput{
			ShardIterator: aws.String("itr-3-1"),
		}).Return(&dynamodbstreams.GetRecordsOutput{
			Records: records3,
		}, nil))

		actual1, err1 := sr.ReadRecords()
		require.NoError(err1)
		require.ElementsMatch(records3, actual1)

		// seek to the beginning
		sr.Seek(records3[0])

		expects = append(expects, client.EXPECT().GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
			ShardId:           aws.String("shard-3"),
			ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeAtSequenceNumber),
			StreamArn:         aws.String("test-arn"),
			SequenceNumber:    records3[0].Dynamodb.SequenceNumber,
		}).Return(&dynamodbstreams.GetShardIteratorOutput{
			ShardIterator: aws.String("itr-3-2"),
		}, nil))

		expects = append(expects, client.EXPECT().GetRecords(&dynamodbstreams.GetRecordsInput{
			ShardIterator: aws.String("itr-3-2"),
		}).Return(&dynamodbstreams.GetRecordsOutput{
			Records: records3,
		}, nil))

		actual2, err2 := sr.ReadRecords()
		require.NoError(err2)
		require.ElementsMatch(records3, actual2)
	}

	// we don't have more shards to read so it will return error
	expects = append(expects, client.EXPECT().DescribeStream(&dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String("test-arn"),
	}).Return(&dynamodbstreams.DescribeStreamOutput{
		StreamDescription: &dynamodbstreams.StreamDescription{
			Shards:    shards, // return the same shards we have read
			StreamArn: aws.String("test-arn"),
		},
	}, nil))

	// no available shards
	_, err = sr.ReadRecords()
	require.ErrorIs(err, ErrNoAvailShards)

	gomock.InOrder(expects...)
}
