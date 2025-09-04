package s3logger

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
}

type S3Logger struct {
	bucket         string
	prefix         string
	fileID         string
	service        S3Client
	batchFrequency time.Duration
	buffer         *bytes.Buffer
	gzWriter       *gzip.Writer
	mutex          sync.Mutex
	maxFileSize    uint
	ticker         *time.Ticker
}

func (l *S3Logger) Sync() {
	l.mutex.Lock()
	var b *bytes.Buffer
	l.gzWriter.Close()
	b, l.buffer = l.buffer, &bytes.Buffer{}
	l.gzWriter = gzip.NewWriter(l.buffer)
	l.mutex.Unlock()
	if len(b.Bytes()) < 1 {
		return
	}
	now := time.Now()
	ctx := context.Background()
	_, err := l.service.PutObject(ctx, &s3.PutObjectInput{
		Body:   bytes.NewReader(b.Bytes()),
		Bucket: aws.String(l.bucket),
		Key:    aws.String(fmt.Sprintf("%s%s/%s-%d.gz", l.prefix, now.Format("2006/01/02/15"), l.fileID, now.UnixMicro())),
	})
	if err != nil {
		fmt.Println(err)
	}
}

func (l *S3Logger) Write(p []byte) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	_, err := l.gzWriter.Write(p)
	if err != nil {
		return err
	}
	if uint(l.buffer.Len()) > l.maxFileSize {
		go func() {
			if l.batchFrequency > 0 {
				l.ticker.Reset(l.batchFrequency)
			}
			l.Sync()
		}()
	}
	return nil
}

type Option func(l *S3Logger) error

func WithPrefix(prefix string) Option {
	return func(l *S3Logger) error {
		l.prefix = prefix
		return nil
	}
}

func WithBatchFrequency(d time.Duration) Option {
	return func(l *S3Logger) error {
		l.batchFrequency = d
		return nil
	}
}

func WithoutBatchFrequency() Option {
	return func(l *S3Logger) error {
		l.batchFrequency = 0
		return nil
	}
}

func WithMaxFileSize(size uint) Option {
	return func(l *S3Logger) error {
		l.maxFileSize = size
		return nil
	}
}

func New(bucket string, service S3Client, opts ...Option) (*S3Logger, error) {
	buffer := &bytes.Buffer{}
	id, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	l := &S3Logger{
		bucket:         bucket,
		service:        service,
		buffer:         buffer,
		fileID:         id.String(),
		gzWriter:       gzip.NewWriter(buffer),
		maxFileSize:    5_000_000,
		batchFrequency: 1 * time.Minute,
	}
	for _, opt := range opts {
		err = opt(l)
		if err != nil {
			return nil, fmt.Errorf("could not apply option: %e", err)
		}
	}
	ctx := context.Background()
	_, err = l.service.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &l.bucket})
	if err != nil {
		return nil, fmt.Errorf("bucket does not exist: %e", err)
	}
	if l.batchFrequency > 0 {
		l.ticker = time.NewTicker(l.batchFrequency)
		l.start()
	}
	return l, nil
}

func (l *S3Logger) start() {
	go func() {
		for range l.ticker.C {
			l.Sync()
		}
	}()
}
