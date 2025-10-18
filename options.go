package leasering

import (
	"io"
	"log/slog"
	"time"
)

// options configures the Ring behavior (internal only).
type options struct {
	ringSize        int
	vnodeCount      int
	leaseTTL        time.Duration
	renewalInterval time.Duration
	refreshInterval time.Duration
	proposalTTL     time.Duration
	joinTimeout     time.Duration
	logger          *slog.Logger
}

// defaultOptions returns sensible defaults.
func defaultOptions() options {
	var (
		leaseTTL        = 15 * time.Second
		refreshInterval = leaseTTL / 2
	)
	return options{
		ringSize:        1024,
		vnodeCount:      8,
		leaseTTL:        leaseTTL,
		renewalInterval: leaseTTL / 3,
		refreshInterval: refreshInterval,
		proposalTTL:     10 * time.Second,
		joinTimeout:     3 * refreshInterval,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// Option is a functional option for configuring a Ring.
type Option func(*options)

// WithLeaseTTL sets the lease time-to-live duration.
func WithLeaseTTL(ttl time.Duration) Option {
	return func(o *options) {
		o.leaseTTL = ttl
		o.renewalInterval = ttl / 3
		o.refreshInterval = ttl / 2
		o.joinTimeout = 3 * o.refreshInterval
	}
}

// WithVNodeCount sets the number of virtual nodes per physical node.
func WithVNodeCount(count int) Option {
	return func(o *options) {
		o.vnodeCount = count
	}
}

// WithLogger sets the logger for the ring.
// If the logger is nil, the ring will use a no-op logger.
// DEFAULT: A no-op logger
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) {
		if logger == nil {
			o.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
			return
		}

		o.logger = logger
	}
}
