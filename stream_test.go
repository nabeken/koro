package koro

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	dynamodbstreams "github.com/aws/aws-sdk-go/service/dynamodbstreams"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const testTableName = "koro-test"

func TestStreamReader(t *testing.T) {
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

func TestDynamoDBStreams(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	require := require.New(t)

	cred := credentials.NewStaticCredentials("koro", "koro", "")
	sess := session.Must(session.NewSession(
		aws.NewConfig().WithRegion("ddblocal").WithEndpoint(os.Getenv("DYNAMODB_ADDR")).WithCredentials(cred),
	))

	tc := &TestClient{
		client: dynamodb.New(sess),
	}

	t.Log("Creating the table...")
	if err := tc.CreateTable(); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		t.Log("Deleting the table...")
		if err := tc.DeleteTable(); err != nil {
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
				tc.DisableStream()
			}
		}
	}()

	streams := dynamodbstreams.New(sess)

	str, err := NewStreamByName(streams, testTableName)
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
	str.Seek(actual1[0])

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
func readAll(sr *StreamReader) ([]*dynamodbstreams.Record, error) {
	var got []*dynamodbstreams.Record

	for {
		records, err := sr.ReadRecords()
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
	client *dynamodb.DynamoDB
}

func (tc *TestClient) CreateTable() error {
	_, err := tc.client.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(testTableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("date"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
			{
				AttributeName: aws.String("date"),
				KeyType:       aws.String(dynamodb.KeyTypeRange),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String(dynamodb.StreamViewTypeNewImage),
		},
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == dynamodb.ErrCodeResourceInUseException {
			return nil
		}
		return fmt.Errorf("creating the testing table: %w", err)
	}

	return tc.client.WaitUntilTableExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(testTableName),
	})
}

func (tc *TestClient) DeleteTable() error {
	_, err := tc.client.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(testTableName),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == dynamodb.ErrCodeResourceNotFoundException {
			return nil
		}
		return fmt.Errorf("deleting the testing table: %w", err)
	}

	return tc.client.WaitUntilTableNotExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(testTableName),
	})
}

func (tc *TestClient) DisableStream() error {
	_, err := tc.client.UpdateTable(&dynamodb.UpdateTableInput{
		TableName: aws.String(testTableName),
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled: aws.Bool(false),
		},
	})
	return err
}

func (tc *TestClient) WriteTestItemNTimes(ntimes int) error {
	for i := 0; i < ntimes; i++ {
		testID := fmt.Sprintf("test-%d", i)
		_, err := tc.client.PutItem(&dynamodb.PutItemInput{
			Item: map[string]*dynamodb.AttributeValue{
				"id": {
					S: aws.String(testID),
				},
				"date": {
					S: aws.String(testID),
				},
				"value": {
					N: aws.String(fmt.Sprintf("%d", i)),
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
