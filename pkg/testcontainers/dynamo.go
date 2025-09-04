package testcontainers

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

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
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if service == dynamodb.ServiceID {
			return aws.Endpoint{
				URL:               fmt.Sprintf("http://localhost:%s", port),
				HostnameImmutable: true,
				PartitionID:       "aws",
				SigningRegion:     "eu-central-1",
			}, nil
		}

		return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
	})
	cfg := aws.NewConfig()
	cfg.EndpointResolverWithOptions = customResolver

	dyndb := dynamodb.NewFromConfig(*cfg, func(options *dynamodb.Options) {
		options.Credentials = aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     "id",
				SecretAccessKey: "secret",
				CanExpire:       false,
			}, nil
		})
	})

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
