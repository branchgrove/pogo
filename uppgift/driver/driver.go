// Package driver defines storage and streaming interfaces for uppgift.
package driver

import (
	"context"
	"errors"
	"time"
)

// ErrLeaseLost is returned by [Execution] methods when the job lease was lost or expired.
var ErrLeaseLost = errors.New("driver: job lease was lost or expired")

// Record contains the persistent state and payload of a single job stored by the [Driver].
type Record struct {
	// ID is the unique identifier of the job.
	ID string
	// Kind is the unique type identifier that routes the job to a handler.
	Kind string
	// Queue is the name of the queue where the record is stored.
	Queue string
	// Payload is the serialized JSON byte data of the job arguments.
	Payload []byte
	// Attributes contains custom metadata key-value pairs attached to the record.
	Attributes map[string]string
	// MaxRetries is the maximum number of retry attempts allowed for the job.
	MaxRetries int
	// Attempt is the current execution attempt count, starting at 0.
	Attempt int
	// Timeout is the maximum duration allowed for a single execution attempt.
	// If zero, the default job timeout (5 minutes) applies.
	Timeout time.Duration
	// RunAt is the scheduled timestamp when the job can be executed.
	RunAt time.Time
	// EnqueuedAt is the timestamp when the record was created.
	EnqueuedAt time.Time
	// UniqueKey is an optional key used for job deduplication.
	UniqueKey string
}

// StreamConfig contains configuration settings to stream jobs to a worker.
type StreamConfig struct {
	WorkerID   string
	BufferSize int
	Queues     []string
}

// Stream delivers job execution leases to a worker.
type Stream interface {
	// Next returns the channel that delivers claimed job executions.
	Next() <-chan Execution
	// Err returns the error that stopped the stream.
	Err() error
	// Close stops the stream and releases stream resources.
	Close() error
}

// Driver provides storage and queue operations for jobs.
type Driver interface {
	// Enqueue inserts one job record into storage.
	Enqueue(ctx context.Context, msg *Record) error
	// EnqueueBatch inserts multiple job records into storage.
	EnqueueBatch(ctx context.Context, msg []*Record) error
	// Stream starts a stream of job executions for a worker.
	Stream(ctx context.Context, cfg StreamConfig) (Stream, error)
}

// Execution represents an active job lease claimed by a worker.
type Execution interface {
	// Record returns the stored job record for this execution.
	Record() *Record
	// Complete marks the execution as finished and deletes the job from storage.
	Complete(ctx context.Context) error
	// Snooze postpones the job until the specified time without incrementing the attempt count.
	Snooze(ctx context.Context, until time.Time) error
	// Fail records a failure and reschedules the job for a retry attempt.
	Fail(ctx context.Context, retryAt time.Time, attempt int, err error) error
	// Discard unschedules the job without further retries.
	Discard(ctx context.Context, err error) error
}
