package s3logger

import (
	"log/slog"
)

type s3LoggerIOWriter struct {
	S3Logger *S3Logger
}

func (l *s3LoggerIOWriter) Write(p []byte) (int, error) {
	err := l.S3Logger.Write(p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func NewSlogJSONS3Logger(s3logger *S3Logger, opts *slog.HandlerOptions) *slog.Logger {
	writer := s3LoggerIOWriter{S3Logger: s3logger}
	handler := slog.NewJSONHandler(&writer, opts)
	return slog.New(handler)
}
