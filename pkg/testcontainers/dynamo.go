package testcontainers

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ory/dockertest/v3"
)

const (
	dynamoImage    = "amazon/dynamodb-local"
	dynamoImageTag = "latest"
)

func StartDynamo(tb testing.TB) *dynamodb.Client {
	tb.Helper()
	pool, _ := dockertest.NewPool("")

	res, err := pool.Run(dynamoImage, dynamoImageTag, []string{})
	if err != nil {
		fmt.Printf("could not start container: %s", err)
		tb.Fatal("could not start container")
	}

	cleanUp := func() {
		_ = pool.Purge(res)
	}
	addCleanup(tb, cleanUp)

	port := res.GetPort("8000/tcp")
	endpoint := fmt.Sprintf("http://localhost:%s", port)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithBaseEndpoint(endpoint),
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     "id",
				SecretAccessKey: "secret",
				CanExpire:       false,
			}, nil
		})),
	)
	if err != nil {
		tb.Fatalf("Could not create AWS config: %s", err)
	}

	dyndb := dynamodb.NewFromConfig(cfg)

	err = pool.Retry(func() error {
		_, err := dyndb.ListTables(context.Background(), &dynamodb.ListTablesInput{})
		if err != nil {
			fmt.Printf("failure during dynamo request: %s\n", err)
		}

		return err
	})
	if err != nil {
		cleanUp()
		fmt.Printf("could not connect to docker container: %s", err)
		panic("could not connect to docker container")
	}

	return dyndb
}

func InitTable(t testing.TB, client *dynamodb.Client, name string) func() {
	t.Helper()

	_, err := client.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("lastUpdated"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("lastUpdated"),
				KeyType:       types.KeyTypeRange,
			},
		},
		TableName: aws.String(name),
	})
	if err != nil {
		t.Fatalf("could not initialize table: %s: %s", name, err)
	}

	return func() {
		DeleteTable(t, client, name)
	}
}

func DeleteTable(t testing.TB, client *dynamodb.Client, name string) {
	t.Helper()

	_, err := client.DeleteTable(t.Context(), &dynamodb.DeleteTableInput{
		TableName: aws.String(name),
	})
	if err != nil {
		t.Fatalf("could not delete table: %s: %s", name, err)
	}
}
