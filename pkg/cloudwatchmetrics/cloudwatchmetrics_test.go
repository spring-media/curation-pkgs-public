package cloudwatchmetrics

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/stretchr/testify/assert"
)

var (
	sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	now = time.Now()
)

const maxTries = 14

func TestCloudWatchMetricSender_Send(t *testing.T) {
	t.Parallel()
	val := float64(now.Unix())

	metricSender, err := New(sess, 0*time.Second)
	assert.NoError(t, err)
	err = metricSender.Send(CloudWatchMetric{
		Namespace:  "test",
		MetricName: "test",
		Unit:       "None",
		Dimensions: nil,
		Value:      val,
	})
	assert.NoError(t, err)

	client := metricSender.cli

	var resp *cloudwatch.GetMetricDataOutput

	err = retry(5*time.Second, maxTries, func() error {
		resp, err = getMetricValues(client, "test", "test")
		if len(resp.MetricDataResults[0].Values) == 0 {
			return fmt.Errorf("no values")
		}
		return err
	})
	assert.NoError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(resp.MetricDataResults))
	assert.Equal(t, val, *resp.MetricDataResults[0].Values[0])
}

func TestCloudWatchMetricSender_Batch(t *testing.T) {
	t.Parallel()
	startVal := float64(now.Unix())

	metricSender, err := New(sess, 1*time.Hour)
	assert.NoError(t, err)

	for i := 0; i < 20; i++ {
		err = metricSender.Send(CloudWatchMetric{
			Namespace:  "test",
			MetricName: "test_batch",
			Unit:       "None",
			Dimensions: nil,
			Value:      startVal + float64(i*1_000),
		})
		assert.NoError(t, err)
	}

	var resp *cloudwatch.GetMetricDataOutput

	client := metricSender.cli
	err = retry(5*time.Second, maxTries, func() error {
		resp, err = getMetricValues(client, "test", "test_batch")
		if len(resp.MetricDataResults[0].Values) == 0 {
			return fmt.Errorf("no values")
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(resp.MetricDataResults))
	assert.Equal(t, startVal+float64(19_000), *resp.MetricDataResults[0].Values[0])
}

func TestCloudWatchMetricSender_BatchFrequency(t *testing.T) {
	t.Parallel()
	val := float64(now.Unix())
	metricSender, err := New(sess, 2*time.Second)

	assert.NoError(t, err)
	err = metricSender.Send(CloudWatchMetric{
		Namespace:  "test",
		MetricName: "test_batch_frequency",
		Unit:       "None",
		Dimensions: nil,
		Value:      val,
	})
	assert.NoError(t, err)

	client := metricSender.cli

	var resp *cloudwatch.GetMetricDataOutput

	err = retry(5*time.Second, maxTries, func() error {
		resp, err = getMetricValues(client, "test", "test_batch_frequency")
		if len(resp.MetricDataResults[0].Values) == 0 {
			return fmt.Errorf("no values")
		}
		return err
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.MetricDataResults))
	assert.Equal(t, val, *resp.MetricDataResults[0].Values[0])
}

func retry(delay time.Duration, numRetries int, f func() error) error {
	for i := 0; i < numRetries; i++ {
		err := f()
		if err == nil {
			return nil
		}
		time.Sleep(delay)
	}
	return fmt.Errorf("retry failed")
}

func getMetricValues(client cloudwatchiface.CloudWatchAPI, namespace, metricName string) (*cloudwatch.GetMetricDataOutput, error) {
	now := time.Now()
	resp, err := client.GetMetricData(&cloudwatch.GetMetricDataInput{
		EndTime:       aws.Time(now),
		LabelOptions:  nil,
		MaxDatapoints: nil,
		MetricDataQueries: []*cloudwatch.MetricDataQuery{
			{
				Expression: aws.String(fmt.Sprintf("SELECT MAX(%s) FROM SCHEMA(%s)", metricName, namespace)),
				Id:         aws.String("test"),
				Label:      aws.String("test"),
				Period:     aws.Int64(60),
				ReturnData: aws.Bool(true),
			},
		},
		StartTime: aws.Time(now.Add(-60 * time.Second)),
	})

	return resp, err
}
