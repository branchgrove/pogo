package uppgift

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"time"
	"uuid"

	"github.com/branchgrove/pogo/uppgift/driver"
)

const (
	defaultQueue      = "default"
	defaultJobTimeout = 5 * time.Minute
	defaultMaxRetries = 25
)

type enqueuerConfig struct {
	defaultJobOptions []JobOption
}

// EnqueuerOption configures an [Enqueuer].
type EnqueuerOption func(*enqueuerConfig)

// Enqueuer serializes jobs and writes them to a [driver.Driver].
type Enqueuer struct {
	driver driver.Driver
	config enqueuerConfig
}

// NewEnqueuer creates an [Enqueuer] with the provided [driver.Driver].
//
// Default job options applied to all enqueued jobs:
//   - Queue: "default" (via [WithQueue])
//   - MaxRetries: 25 (via [WithMaxRetries])
//   - Timeout: 5 minutes (via [WithTimeout])
func NewEnqueuer(d driver.Driver, opts ...EnqueuerOption) *Enqueuer {
	cfg := enqueuerConfig{
		defaultJobOptions: []JobOption{
			WithQueue(defaultQueue),
			WithMaxRetries(defaultMaxRetries),
			WithTimeout(defaultJobTimeout),
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	return &Enqueuer{
		driver: d,
		config: cfg,
	}
}

// WithDefaultJobOptions sets default [JobOption] values applied to all jobs enqueued
// by the [Enqueuer].
func WithDefaultJobOptions(opts ...JobOption) EnqueuerOption {
	return func(c *enqueuerConfig) {
		c.defaultJobOptions = append(c.defaultJobOptions, opts...)
	}
}

// Enqueue serializes and enqueues a job with the provided options using the underlying [driver.Driver].
// It returns the job ID.
//
// Option values are resolved with the following precedence (lowest to highest):
//  1. Default options set on the [Enqueuer] (defaults: Queue="default", MaxRetries=25, Timeout=5m)
//  2. Options returned by [JobDefaults.JobOptions] if the job implements [JobDefaults]
//  3. Options passed directly in opts
//
// If no execution time is configured with [WithRunAt] or [WithDelay], the job runs immediately.
func (c *Enqueuer) Enqueue(ctx context.Context, job JobArgs, opts ...JobOption) (string, error) {
	cfg := jobConfig{}

	if c.config.defaultJobOptions != nil {
		for _, opt := range c.config.defaultJobOptions {
			opt(&cfg)
		}
	}

	if def, ok := job.(JobDefaults); ok {
		for _, opt := range def.JobOptions() {
			opt(&cfg)
		}
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	payload, err := json.Marshal(job)
	if err != nil {
		return "", fmt.Errorf("uppgift: failed to marshal job for kind=%q: %w", job.Kind(), err)
	}

	now := time.Now().UTC()
	id := uuid.New().String()

	runAt := cfg.runAt
	if runAt.IsZero() {
		runAt = now
	}

	record := driver.Record{
		ID:         id,
		Kind:       job.Kind(),
		Queue:      cfg.queue,
		Attributes: cfg.attributes,
		MaxRetries: cfg.maxRetries,
		Attempt:    0,
		Timeout:    cfg.timeout,
		RunAt:      runAt,
		UniqueKey:  cfg.uniqueKey,
		Payload:    payload,
		EnqueuedAt: now,
	}

	err = c.driver.Enqueue(ctx, &record)
	if err != nil {
		return "", err
	}

	return id, nil
}

// TODO: implement EnqueueBatch, it should deduplicate uniquekeys
