package retryqueue

import "time"

var defaultMaxAttempts = 10
var defaultScanInterval = 500 * time.Millisecond
var defaultLeaseDur = 30 * time.Second

type Option func(*queueConfig)

type queueConfig struct {
	logger       Logger
	scanInterval time.Duration
	leaseDul     time.Duration
	maxAttemts   int
	metrics      Metrics
}

func defalutQueueConfig() queueConfig {
	return queueConfig{
		logger:       NewDefaultLogger(),
		scanInterval: defaultScanInterval,
		leaseDul:     defaultLeaseDur,
		maxAttemts:   defaultMaxAttempts,
		metrics:      noopMetrics{},
	}
}

func WithLogger(logger Logger) Option {
	return func(c *queueConfig) {
		if logger != nil {
			c.logger = logger
		}
	}
}

func WithScanInterval(d time.Duration) Option {
	return func(c *queueConfig) {
		if d > 0 {
			c.scanInterval = d
		}
	}
}

func WithLeaseDul(d time.Duration) Option {
	return func(c *queueConfig) {
		if d > 0 {
			c.leaseDul = d
		}
	}
}

func WithMacAttempts(attempt int) Option {
	return func(c *queueConfig) {
		if attempt > 0 {
			c.maxAttemts = attempt
		}
	}
}

func WithMetrics(m Metrics) Option {
	return func(c *queueConfig) {
		c.metrics = m
	}

}
