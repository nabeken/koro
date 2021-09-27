package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/nabeken/koro"
)

var (
	ErrEndOfShard = errors.New("koro: End of Shard")
)

const testTableName = "koro-test"

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

func (tc *TestClient) WriteTestItemNTimes(ntimes int) error {
	for i := 0; i < ntimes; i++ {
		testID := fmt.Sprintf("%s", time.Now())
		_, err := tc.client.PutItem(&dynamodb.PutItemInput{
			Item: map[string]*dynamodb.AttributeValue{
				"id": {
					S: aws.String(testID),
				},
				"date": {
					S: aws.String(testID),
				},
				"date2": {
					S: aws.String("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
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
		log.Print("1 item wrote.")
	}
	return nil
}

func main() {
	cred := credentials.NewStaticCredentials(os.Getenv("DYNAMODB_AWS_ACCESS_KEY"), os.Getenv("DYNAMODB_AWS_SECRET_KEY"), "")
	sess := session.Must(session.NewSession(
		aws.NewConfig().WithRegion("ddblocal").WithEndpoint(os.Getenv("DYNAMODB_ADDR")).WithCredentials(cred),
	))

	dc := dynamodb.New(sess)

	tc := &TestClient{
		client: dc,
	}

	if err := tc.CreateTable(); err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			const nTimes = 1000
			log.Print("Writing items...")
			if err := tc.WriteTestItemNTimes(nTimes); err != nil {
				log.Fatal(err)
			}
			log.Printf("%d items wrote", nTimes)
		}
	}()

	streams := dynamodbstreams.New(sess)

	sr, err := koro.NewStreamByName(streams, testTableName)
	if err != nil {
		log.Fatal(err)
	}

	for {
		log.Printf("reading a shard of %s", sr.Reader().ShardID())

		records, err := sr.ReadRecords()
		if err != nil {
			log.Fatal(err)
		}

		if len(records) == 0 {
			log.Print("no records found.")
			time.Sleep(time.Second)
		}

		log.Printf("%d records read", len(records))
	}
}
