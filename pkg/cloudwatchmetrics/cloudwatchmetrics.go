package cloudwatchmetrics

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
	"go.uber.org/zap"
)

type CloudWatchMetric struct {
	Namespace  string
	MetricName string
	Unit       string
	Dimensions []struct {
		Name  string
		Value string
	}
	Value float64
}

type (
	CloudWatchMetricSender struct {
		cli    cloudwatchiface.CloudWatchAPI
		m      sync.Mutex
		ch     chan channelItem
		batch  map[string][]*cloudwatch.MetricDatum
		logger *zap.Logger
	}

	channelItem struct {
		nameSpace   string
		metricDatum *cloudwatch.MetricDatum
	}
)

const maxMetricsPerRequest = 20

func (s *CloudWatchMetricSender) Send(m CloudWatchMetric) error {
	md := &cloudwatch.MetricDatum{
		MetricName: aws.String(m.MetricName),
		Unit:       aws.String(m.Unit),
		Value:      aws.Float64(m.Value),
		Dimensions: []*cloudwatch.Dimension{},
	}

	for _, d := range m.Dimensions {
		md.Dimensions = append(md.Dimensions, &cloudwatch.Dimension{
			Name:  aws.String(d.Name),
			Value: aws.String(d.Value),
		})
	}

	if s.ch != nil {
		s.ch <- channelItem{
			nameSpace:   m.Namespace,
			metricDatum: md,
		}
		return nil
	}

	s.m.Lock()
	defer s.m.Unlock()

	_, err := s.cli.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace:  aws.String(m.Namespace),
		MetricData: []*cloudwatch.MetricDatum{md},
	})
	if err != nil {
		return err
	}

	return nil
}

func New(sess *session.Session, batchFrequency time.Duration, opts ...func(l *CloudWatchMetricSender)) (*CloudWatchMetricSender, error) {
	zapConf := zap.NewProductionConfig()
	logger, err := zapConf.Build(zap.AddStacktrace(zap.FatalLevel))
	if err != nil {
		return nil, err
	}
	l := &CloudWatchMetricSender{
		cli:    cloudwatch.New(sess),
		logger: logger,
	}

	for _, opt := range opts {
		opt(l)
	}

	if batchFrequency > 0 {
		l.ch = make(chan channelItem, 10000)
		l.batch = map[string][]*cloudwatch.MetricDatum{}
		t := time.NewTicker(batchFrequency)

		go l.sendBatches(t.C)
	}

	return l, nil
}

func (s *CloudWatchMetricSender) sendBatches(ticker <-chan time.Time) {
	for {
		select {
		case p := <-s.ch:
			s.batch[p.nameSpace] = append(s.batch[p.nameSpace], p.metricDatum)
			if len(s.batch[p.nameSpace]) >= maxMetricsPerRequest {
				go s.sendBatch(p.nameSpace, s.batch[p.nameSpace])
			}
		case <-ticker:
			s.Sync()
		}
	}
}

func (s *CloudWatchMetricSender) Sync() {
	for n, b := range s.batch {
		go s.sendBatch(n, b)
		s.batch[n] = nil
	}
}

func (s *CloudWatchMetricSender) sendBatch(namespace string, batch []*cloudwatch.MetricDatum) {
	s.m.Lock()
	defer s.m.Unlock()

	if len(batch) == 0 {
		return
	}

	var metrics []*cloudwatch.MetricDatum

	for i, md := range batch {
		metrics = append(metrics, md)

		if (i+1)%maxMetricsPerRequest == 0 || i == (len(batch)-1) {
			res, err := s.cli.PutMetricData(&cloudwatch.PutMetricDataInput{
				Namespace:  aws.String(namespace),
				MetricData: metrics,
			})
			if err != nil {
				s.logger.Warn("failed to PutMetricData", zap.String("namespace", namespace), zap.String("response", res.String()), zap.Error(err))
			}
			metrics = nil
		}
	}
}

func WithLogger(logger *zap.Logger) func(l *CloudWatchMetricSender) {
	return func(l *CloudWatchMetricSender) {
		l.logger = logger
	}
}
