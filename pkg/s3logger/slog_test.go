package s3logger

import (
	"compress/gzip"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSLog(t *testing.T) {
	client := s3MockClient{debugChan: make(chan interface{}, 1)}
	l, err := New("foundry-curation-test", client, WithoutBatchFrequency())
	require.NoError(t, err)

	// Create a new slog.Logger
	logger := NewSlogJSONS3Logger(l, nil)

	// Test logging a message
	logger.Warn("Warning Test", slog.String("time", "overwritten"))
	logger.Info("Test Message", slog.Bool("some bool", true), slog.String("time", "overwritten"))

	l.Sync()

	rs := (<-client.debugChan).(s3.PutObjectInput)
	// uncompress gzip body
	reader, err := gzip.NewReader(rs.Body)
	assert.Nil(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	assert.Nil(t, err)

	lines := strings.Split(string(data), "\n")
	assert.Len(t, lines, 3)

	assert.JSONEq(t, `{"time":"overwritten","level":"WARN","msg":"Warning Test"}`, lines[0])
	assert.JSONEq(t, `{"time":"overwritten","level":"INFO","msg":"Test Message","some bool":true}`, lines[1])
}
