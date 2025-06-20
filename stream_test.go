package koro

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

const testTableName = "koro-test"

func TestStreamReader(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	client := NewMockDynamoDBStreamer(ctrl)
	shards := []types.Shard{
		{
			SequenceNumberRange: &types.SequenceNumberRange{
				StartingSequenceNumber: aws.String("0000"),
				EndingSequenceNumber:   aws.String("0001"),
			},
			ShardId: aws.String("shard-1"),
		},
		{
			ParentShardId: aws.String("shard-1"),
			SequenceNumberRange: &types.SequenceNumberRange{
				StartingSequenceNumber: aws.String("0002"),
				EndingSequenceNumber:   aws.String("0003"),
			},
			ShardId: aws.String("shard-2"),
		},
	}

	var expects []any

	// should request an iterator for the latest record
	expects = append(expects, client.EXPECT().DescribeStream(gomock.Any(), &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String("test-arn"),
	}).Return(&dynamodbstreams.DescribeStreamOutput{
		StreamDescription: &types.StreamDescription{
			Shards:    shards,
			StreamArn: aws.String("test-arn"),
		},
	}, nil))

	sr, err := NewStreamReader(context.Background(), client, aws.String("test-arn"))
	require.NoError(err)

	// reading shard-1
	{
		expects = append(expects, client.EXPECT().GetShardIterator(gomock.Any(), &dynamodbstreams.GetShardIteratorInput{
			ShardId:           aws.String("shard-1"),
			ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
			StreamArn:         aws.String("test-arn"),
		}).Return(&dynamodbstreams.GetShardIteratorOutput{
			ShardIterator: aws.String("itr-1-1"),
		}, nil))

		expects = append(expects, client.EXPECT().GetRecords(gomock.Any(), &dynamodbstreams.GetRecordsInput{
			ShardIterator: aws.String("itr-1-1"),
		}).Return(&dynamodbstreams.GetRecordsOutput{
			Records:           records1,
			NextShardIterator: aws.String("itr-1-2"),
		}, nil))

		actual1, err1 := sr.ReadRecords(context.Background())
		require.NoError(err1)
		require.ElementsMatch(records1, actual1)

		expects = append(expects, client.EXPECT().GetRecords(gomock.Any(), &dynamodbstreams.GetRecordsInput{
			ShardIterator: aws.String("itr-1-2"),
		}).Return(&dynamodbstreams.GetRecordsOutput{
			Records: records2,
		}, nil))
		actual2, err2 := sr.ReadRecords(context.Background())
		require.NoError(err2)
		require.ElementsMatch(records2, actual2)
	}

	// reading shard-2
	{
		expects = append(expects, client.EXPECT().GetShardIterator(gomock.Any(), &dynamodbstreams.GetShardIteratorInput{
			ShardId:           aws.String("shard-2"),
			ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
			StreamArn:         aws.String("test-arn"),
		}).Return(&dynamodbstreams.GetShardIteratorOutput{
			ShardIterator: aws.String("itr-2-1"),
		}, nil))

		expects = append(expects, client.EXPECT().GetRecords(gomock.Any(), &dynamodbstreams.GetRecordsInput{
			ShardIterator: aws.String("itr-2-1"),
		}).Return(&dynamodbstreams.GetRecordsOutput{
			Records:           records1,
			NextShardIterator: aws.String("itr-2-2"),
		}, nil))

		actual1, err1 := sr.ReadRecords(context.Background())
		require.NoError(err1)
		require.ElementsMatch(records1, actual1)

		expects = append(expects, client.EXPECT().GetRecords(gomock.Any(), &dynamodbstreams.GetRecordsInput{
			ShardIterator: aws.String("itr-2-2"),
		}).Return(&dynamodbstreams.GetRecordsOutput{
			Records: records2,
		}, nil))
		actual2, err2 := sr.ReadRecords(context.Background())
		require.NoError(err2)
		require.ElementsMatch(records2, actual2)
	}

	// reading shard-3 that was added after the first attempt
	{
		shards = append(shards, types.Shard{
			ParentShardId: aws.String("shard-2"),
			SequenceNumberRange: &types.SequenceNumberRange{
				StartingSequenceNumber: aws.String("0004"),
				EndingSequenceNumber:   aws.String("0005"),
			},
			ShardId: aws.String("shard-3"),
		})

		// should request an iterator for the latest record
		expects = append(expects, client.EXPECT().DescribeStream(gomock.Any(), &dynamodbstreams.DescribeStreamInput{
			StreamArn: aws.String("test-arn"),
		}).Return(&dynamodbstreams.DescribeStreamOutput{
			StreamDescription: &types.StreamDescription{
				Shards:    shards,
				StreamArn: aws.String("test-arn"),
			},
		}, nil))

		expects = append(expects, client.EXPECT().GetShardIterator(gomock.Any(), &dynamodbstreams.GetShardIteratorInput{
			ShardId:           aws.String("shard-3"),
			ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
			StreamArn:         aws.String("test-arn"),
		}).Return(&dynamodbstreams.GetShardIteratorOutput{
			ShardIterator: aws.String("itr-3-1"),
		}, nil))

		expects = append(expects, client.EXPECT().GetRecords(gomock.Any(), &dynamodbstreams.GetRecordsInput{
			ShardIterator: aws.String("itr-3-1"),
		}).Return(&dynamodbstreams.GetRecordsOutput{
			Records: records3,
		}, nil))

		actual1, err1 := sr.ReadRecords(context.Background())
		require.NoError(err1)
		require.ElementsMatch(records3, actual1)

		// seek to the beginning
		sr.Seek(&records3[0])

		expects = append(expects, client.EXPECT().GetShardIterator(gomock.Any(), &dynamodbstreams.GetShardIteratorInput{
			ShardId:           aws.String("shard-3"),
			ShardIteratorType: types.ShardIteratorTypeAtSequenceNumber,
			StreamArn:         aws.String("test-arn"),
			SequenceNumber:    records3[0].Dynamodb.SequenceNumber,
		}).Return(&dynamodbstreams.GetShardIteratorOutput{
			ShardIterator: aws.String("itr-3-2"),
		}, nil))

		expects = append(expects, client.EXPECT().GetRecords(gomock.Any(), &dynamodbstreams.GetRecordsInput{
			ShardIterator: aws.String("itr-3-2"),
		}).Return(&dynamodbstreams.GetRecordsOutput{
			Records: records3,
		}, nil))

		actual2, err2 := sr.ReadRecords(context.Background())
		require.NoError(err2)
		require.ElementsMatch(records3, actual2)
	}

	// we don't have more shards to read so it will return error
	expects = append(expects, client.EXPECT().DescribeStream(gomock.Any(), &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String("test-arn"),
	}).Return(&dynamodbstreams.DescribeStreamOutput{
		StreamDescription: &types.StreamDescription{
			Shards:    shards, // return the same shards we have read
			StreamArn: aws.String("test-arn"),
		},
	}, nil))

	// no available shards
	_, err = sr.ReadRecords(context.Background())
	require.ErrorIs(err, ErrNoAvailShards)

	gomock.InOrder(expects...)
}

func TestDynamoDBStreams(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	ctx := context.Background()

	require := require.New(t)

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				"koro", "koro", "",
			),
		),

		config.WithRegion("us-west-2"),
	)

	cfg.BaseEndpoint = aws.String(os.Getenv("DYNAMODB_ADDR"))

	require.NoError(err)

	tc := &TestClient{
		client: dynamodb.NewFromConfig(cfg),
	}

	t.Log("Creating the table...")
	if err := tc.CreateTable(ctx); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		t.Log("Deleting the table...")
		if err := tc.DeleteTable(ctx); err != nil {
			t.Log(err)
		}
	})

	const nItems = 1000
	go func() {
		for i := 0; ; i++ {
			t.Log("Writing items...")
			if err := tc.WriteTestItemNTimes(nItems); err != nil {
				t.Log(err)
				return
			}
			t.Logf("%d items wrote", nItems)
			if i == 0 {
				tc.DisableStream(ctx)
			}
		}
	}()

	streams := dynamodbstreams.NewFromConfig(cfg)

	str, err := NewStreamByName(context.Background(), streams, testTableName)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Reading the shard...")
	actual1, err1 := readAll(str)
	if err1 != nil {
		t.Fatal(err1)
	}
	require.Len(actual1, 1000)

	// seek to the beginning
	t.Log("Seeking to the beginning...")
	str.Seek(&actual1[0])

	t.Log("Reading the shard again from the beginning...")
	actual2, err2 := readAll(str)
	if err != nil {
		t.Fatal(err2)
	}

	require.Len(actual2, 1000)
	require.ElementsMatch(actual1, actual2)
}

// readAll reads all records until it gets ErrNoAvailShards or unexpected error.
// If err == ErrNoAvailShards, err will be nil.
func readAll(sr *StreamReader) ([]types.Record, error) {
	var got []types.Record

	for {
		records, err := sr.ReadRecords(context.Background())
		if err != nil {
			if errors.Is(err, ErrNoAvailShards) {
				break
			}
			return nil, err
		}

		if len(records) == 0 {
			time.Sleep(time.Second)
		}

		got = append(got, records...)
	}

	return got, nil
}

// TestClient implements utility methods for the testing.
type TestClient struct {
	client *dynamodb.Client
}

func (tc *TestClient) CreateTable(ctx context.Context) error {
	_, err := tc.client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(testTableName),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: ddbtypes.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("date"),
				AttributeType: ddbtypes.ScalarAttributeTypeS,
			},
		},
		KeySchema: []ddbtypes.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       ddbtypes.KeyTypeHash,
			},
			{
				AttributeName: aws.String("date"),
				KeyType:       ddbtypes.KeyTypeRange,
			},
		},
		ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: ddbtypes.StreamViewTypeNewImage,
		},
	})
	if err != nil {
		var awsErr *ddbtypes.ResourceInUseException
		if errors.As(err, &awsErr) {
			return nil
		}
		return fmt.Errorf("creating the testing table: %w", err)
	}

	waiter := dynamodb.NewTableExistsWaiter(tc.client)

	return waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(testTableName),
	}, 5*time.Minute)
}

func (tc *TestClient) DeleteTable(ctx context.Context) error {
	_, err := tc.client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(testTableName),
	})
	if err != nil {
		var awsErr *ddbtypes.ResourceNotFoundException
		if errors.As(err, &awsErr) {
			return nil
		}
		return fmt.Errorf("deleting the testing table: %w", err)
	}

	waiter := dynamodb.NewTableNotExistsWaiter(tc.client)

	return waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(testTableName),
	}, 5*time.Minute)
}

func (tc *TestClient) DisableStream(ctx context.Context) error {
	_, err := tc.client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String(testTableName),
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled: aws.Bool(false),
		},
	})
	return err
}

func (tc *TestClient) WriteTestItemNTimes(ntimes int) error {
	ctx := context.Background()

	for i := range ntimes {
		testID := fmt.Sprintf("test-%d", i)
		_, err := tc.client.PutItem(ctx, &dynamodb.PutItemInput{
			Item: map[string]ddbtypes.AttributeValue{
				"id": &ddbtypes.AttributeValueMemberS{
					Value: testID,
				},
				"date": &ddbtypes.AttributeValueMemberS{
					Value: testID,
				},
				"value": &ddbtypes.AttributeValueMemberN{
					Value: fmt.Sprintf("%d", i),
				},
			},
			TableName: aws.String(testTableName),
		})
		if err != nil {
			return fmt.Errorf("putting an item: %w", err)
		}
	}
	return nil
}
