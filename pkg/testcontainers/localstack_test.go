package testcontainers

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestLocalStack(t *testing.T) {
	ctx := context.Background()

	ls := StartLocalStack(t, []Service{ServiceS3, ServiceDynamoDB})

	t.Run("S3 Client", func(t *testing.T) {
		s3Cli := ls.GetS3Client()

		require.NotNil(t, s3Cli, "S3 client should not be nil")

		_, err := s3Cli.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String("test-bucket"),
		})
		require.NoError(t, err, "Failed to create bucket")

		buckets, err := s3Cli.ListBuckets(ctx, &s3.ListBucketsInput{})
		require.NoError(t, err, "Failed to list buckets")
		require.Len(t, buckets.Buckets, 1, "There should be one bucket")
		require.Equal(t, "test-bucket", *buckets.Buckets[0].Name, "Bucket name should match")
	})

	t.Run("DynamoDB Client", func(t *testing.T) {
		dynamoCli := ls.GetDynamoDBClient()

		require.NotNil(t, dynamoCli, "DynamoDB client should not be nil")

		// Example operation: Create a table
		_, err := dynamoCli.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String("test-table"),
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String("id"),
					KeyType:       types.KeyTypeHash,
				},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String("id"),
					AttributeType: types.ScalarAttributeTypeS,
				},
			},
			BillingMode: types.BillingModePayPerRequest,
		})
		require.NoError(t, err, "Failed to create table")

		// Example operation: List tables
		tables, err := dynamoCli.ListTables(ctx, &dynamodb.ListTablesInput{})
		require.NoError(t, err, "Failed to list tables")
		require.Len(t, tables.TableNames, 1, "There should be one table")
		require.Equal(t, "test-table", tables.TableNames[0], "Table name should match")
	})
}
