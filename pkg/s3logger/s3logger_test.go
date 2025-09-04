package s3logger

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/stretchr/testify/assert"
)

type s3MockClient struct {
	debugChan chan interface{}
}

func (c s3MockClient) HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return nil, nil
}

func (c s3MockClient) PutObject(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if c.debugChan != nil {
		c.debugChan <- params
	}
	return nil, nil
}

func TestNewS3Logger(t *testing.T) {
	l, err := New("foundry-curation-test", s3MockClient{}, WithoutBatchFrequency())
	assert.Nil(t, err)
	assert.NotNil(t, l)

	for i := 0; i < 1_000_000; i++ {
		data := []byte(fmt.Sprintf("This is line number %d", i))
		_ = l.Write(data)
	}
	l.Sync()
	assert.Zero(t, l.buffer.Len())
}

func TestConcurrency(t *testing.T) {
	l, err := New("foundry-curation-test", s3MockClient{}, WithoutBatchFrequency())
	assert.Nil(t, err)
	assert.NotNil(t, l)

	for i := 0; i < 1_000_000; i++ {
		data := []byte(fmt.Sprintf("This is line number %d", i))
		_ = l.Write(data)
		if i%1_000 == 0 {
			go func() { l.Sync() }()
		}
	}
	l.Sync()
	assert.Zero(t, l.buffer.Len())
}

func TestSyncAfter50M(t *testing.T) {
	client := s3MockClient{
		debugChan: make(chan interface{}),
	}
	l, err := New("foundry-curation-test", client, WithoutBatchFrequency())
	assert.Nil(t, err)
	assert.NotNil(t, l)

	assert.Len(t, client.debugChan, 0)

	messageBytes := []byte("' !\"#$%&\\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~'")
	for i := 0; uint(l.buffer.Len()) < l.maxFileSize; i++ {
		l.Write(messageBytes)
	}
	rs := (<-client.debugChan).(*s3.PutObjectInput)
	data, err := io.ReadAll(rs.Body)
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, uint(len(data)), l.maxFileSize)
	assert.LessOrEqual(t, uint(len(data)), uint(1.1*float64(l.maxFileSize)))
}
