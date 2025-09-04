package testcontainers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

type Service = string

const (
	ServiceS3             Service = "s3"
	ServiceCloudWatchLogs Service = "logs"
	ServiceDynamoDB       Service = "dynamodb"
	ServiceSSM            Service = "ssm"
)

type LocalStack struct {
	resource *dockertest.Resource
	cfg      aws.Config
	services []Service
	tb       testing.TB
}

type ServiceStatus string

const (
	ServiceAvailable ServiceStatus = "available"
)

type Services map[string]ServiceStatus

type HealthResponse struct {
	Services Services `json:"services"`
	Edition  string   `json:"edition"`
	Version  string   `json:"version"`
}

func (s ServiceStatus) Available() bool {
	return s == ServiceAvailable
}

func (ts *LocalStack) GetS3Client() *s3.Client {
	if !slices.Contains(ts.services, ServiceS3) {
		ts.tb.Fatalf("S3 service is not included in the Localstack instance")
	}
	return s3.NewFromConfig(ts.cfg, func(o *s3.Options) {
		o.UsePathStyle = true // LocalStack requires path-style access for S3
	})
}

func (ts *LocalStack) GetCloudWatchLogsClient() *cloudwatchlogs.Client {
	if !slices.Contains(ts.services, ServiceCloudWatchLogs) {
		ts.tb.Fatalf("CloudWatchLogs service is not included in the Localstack instance")
	}
	return cloudwatchlogs.NewFromConfig(ts.cfg)
}

func (ts *LocalStack) GetDynamoDBClient() *dynamodb.Client {
	if !slices.Contains(ts.services, ServiceDynamoDB) {
		ts.tb.Fatalf("DynamoDB service is not included in the Localstack instance")
	}
	return dynamodb.NewFromConfig(ts.cfg)
}

func (ts *LocalStack) GetSSMClient() *ssm.Client {
	if !slices.Contains(ts.services, ServiceSSM) {
		ts.tb.Fatalf("SSM service is not included in the Localstack instance")
	}
	return ssm.NewFromConfig(ts.cfg)
}

func (ts *LocalStack) GetConfig() aws.Config {
	return ts.cfg
}

func (ts *LocalStack) IsRunning() bool {
	return ts.resource.Container.State.Running
}

func StartLocalStack(tb testing.TB, services []Service) *LocalStack {
	tb.Helper()

	pool, err := dockertest.NewPool("")
	if err != nil {
		tb.Fatalf("Could not connect to docker: %s", err)
	}

	// Run LocalStack container
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "localstack/localstack",
		Tag:        "4.7",
		Env: []string{
			"SERVICES=" + strings.Join(services, ","),
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		tb.Fatalf("Could not start resource: %s", err)
	}

	addCleanup(tb, func() {
		if err := pool.Purge(resource); err != nil {
			tb.Logf("Could not purge resource: %s", err)
		}
	})

	port := resource.GetPort("4566/tcp")

	// Wait for LocalStack to be ready
	pool.MaxWait = 120 * time.Second
	fmt.Printf("waiting for LocalStack at http://localhost:%s\n", port)
	if err := pool.Retry(func() error {
		url := fmt.Sprintf("http://localhost:%s/_localstack/health", port)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status code not OK")
		}

		var healthResp HealthResponse
		err = json.NewDecoder(resp.Body).Decode(&healthResp)
		var errs []error
		for _, service := range services {
			if !healthResp.Services[service].Available() {
				errs = append(errs, fmt.Errorf("%s not running", service))
			}
		}
		if err := errors.Join(errs...); err != nil {
			return fmt.Errorf("services not running: %w", err)
		}
		return nil
	}); err != nil {
		tb.Fatalf("Could not connect to LocalStack: %s", err)
	}

	// Create AWS config for LocalStack
	endpoint := fmt.Sprintf("http://localhost:%s", port)
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithBaseEndpoint(endpoint),
		config.WithRegion("us-east-1"),
		// use fake credentials for LocalStack (anonymous credentials are not working for s3)
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "112233445566", SecretAccessKey: "112233445566"}, nil
		})),
	)
	if err != nil {
		tb.Fatalf("Could not create AWS config: %s", err)
	}

	return &LocalStack{
		resource: resource,
		cfg:      cfg,
		services: services,
		tb:       tb,
	}
}

// addCleanup registers a cleanup function to the test and creates a go routine to call the cleanup function on os.Interrupt, syscall.SIGTERM, syscall.SIGKILL.
func addCleanup(tb testing.TB, fn func()) {
	tb.Helper()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fn()
	}()

	tb.Cleanup(fn)
}
