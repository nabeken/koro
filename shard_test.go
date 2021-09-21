package koro

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/stretchr/testify/assert"
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
