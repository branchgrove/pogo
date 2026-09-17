package uppgift

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"time"
	"uuid"

	"github.com/branchgrove/pogo/uppgift/driver"
)

const (
	defaultMinBackoff      = 1 * time.Second
	defaultMaxBackoff      = 24 * time.Hour
	defaultShutdownTimeout = 25 * time.Second
	defaultConcurrency     = 32
	defaultAckTimeout      = 3 * time.Second
)

// BackoffFunc calculates the retry delay for a given attempt number.
type BackoffFunc func(attempt int) time.Duration

// Worker listens for jobs in configured queues and executes them with a [Mux].
type Worker struct {
	driver driver.Driver
	mux    *Mux
	config workerConfig
}

type workerConfig struct {
	calculateBackoff BackoffFunc
	concurrency      int
	queues           []string
	id              string
	shutdownTimeout time.Duration
}

// WorkerOption configures a [Worker].
type WorkerOption func(*workerConfig)

// WithBackoffFunc sets the function used to calculate retry delay durations.
// The default calculation uses exponential backoff with equal jitter from 1 second up to 24 hours.
func WithBackoffFunc(f BackoffFunc) WorkerOption {
	return func(c *workerConfig) {
		c.calculateBackoff = f
	}
}

// WithConcurrency sets the maximum number of concurrent running jobs.
// The default concurrency is 32.
func WithConcurrency(n int) WorkerOption {
	return func(c *workerConfig) {
		c.concurrency = n
	}
}

// WithQueues sets the queues from which the worker receives jobs.
// The default queue is "default".
func WithQueues(queues ...string) WorkerOption {
	return func(c *workerConfig) {
		c.queues = queues
	}
}

// WithID sets the worker identifier. The identifier must be globally unique
// and not reused across restarts.
// The default identifier is a newly generated random UUID.
func WithID(id string) WorkerOption {
	return func(c *workerConfig) {
		c.id = id
	}
}

// WithShutdownTimeout sets the maximum duration to wait for in-flight jobs to
// complete when the context is canceled.
// The default duration is 25 seconds.
func WithShutdownTimeout(d time.Duration) WorkerOption {
	return func(c *workerConfig) {
		c.shutdownTimeout = d
	}
}

func defaultBackoffFunc(attempt int) time.Duration {
	if attempt <= 0 {
		return defaultMinBackoff
	}

	// Clamp attempt to prevent int64 bit-shift overflow (1 << 30 is ~34 years).
	if attempt > 30 {
		attempt = 30
	}

	// Exponential delay: min * 2^(attempt - 1)
	backoff := defaultMinBackoff * (1 << (attempt - 1))
	if backoff > defaultMaxBackoff || backoff <= 0 {
		backoff = defaultMaxBackoff
	}

	// AWS Equal Jitter: backoff/2 + rand(0, backoff/2)
	// - Guarantees delay is always at least 50% of the exponential curve.
	// - Randomizes the remaining 50% to prevent thundering herd spikes.
	half := backoff / 2
	if half <= 0 {
		return backoff
	}

	return half + rand.N(half)
}

// NewWorker creates a [Worker] with the provided [driver.Driver] and [Mux].
//
// Default configuration:
//   - Concurrency: 32 (via [WithConcurrency])
//   - Queues: []string{"default"} (via [WithQueues])
//   - ID: A newly generated random UUID (via [WithID])
//   - ShutdownTimeout: 25 seconds (via [WithShutdownTimeout])
//   - BackoffFunc: Exponential backoff with equal jitter from 1 second up to 24 hours (via [WithBackoffFunc])
func NewWorker(d driver.Driver, mux *Mux, opts ...WorkerOption) *Worker {
	w := &Worker{
		driver: d,
		mux:    mux,
	}

	for _, opt := range opts {
		opt(&w.config)
	}

	if w.config.calculateBackoff == nil {
		w.config.calculateBackoff = defaultBackoffFunc
	}

	if w.config.concurrency <= 0 {
		w.config.concurrency = defaultConcurrency
	}

	if len(w.config.queues) == 0 {
		w.config.queues = []string{defaultQueue}
	}

	if w.config.id == "" {
		w.config.id = uuid.New().String()
	}

	if w.config.shutdownTimeout <= 0 {
		w.config.shutdownTimeout = defaultShutdownTimeout
	}

	return w
}

// Start begins listening for and executing jobs from the underlying [driver.Driver].
func (w *Worker) Start(ctx context.Context) error {
	stream, err := w.driver.Stream(ctx, driver.StreamConfig{
		WorkerID:   w.config.id,
		BufferSize: w.config.concurrency,
		Queues:     w.config.queues,
	})
	if err != nil {
		return fmt.Errorf("uppgift: failed to start worker stream: %w", err)
	}
	defer stream.Close()

	jobsCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()

	sem := make(chan struct{}, w.config.concurrency)
	var wg sync.WaitGroup
	var streamErr error

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case sem <- struct{}{}:
			select {
			case <-ctx.Done():
				<-sem
				break loop

			case exec, ok := <-stream.Next():
				if !ok {
					<-sem
					streamErr = stream.Err()
					break loop
				}

				wg.Add(1)
				go func(e driver.Execution) {
					defer func() {
						<-sem
						wg.Done()
					}()

					// TODO: handle potential processing error from ack
					w.process(jobsCtx, e)
				}(exec)
			}
		}
	}

	shutdownTimer := time.AfterFunc(w.config.shutdownTimeout, cancelJobs)
	defer shutdownTimer.Stop()

	wg.Wait()

	if streamErr != nil {
		return fmt.Errorf("uppgift: stream error: %w", streamErr)
	}

	return ctx.Err()
}

func (w *Worker) process(ctx context.Context, exec driver.Execution) error {
	record := exec.Record()
	meta := Meta{
		ID:         record.ID,
		Attributes: record.Attributes,
		Attempt:    record.Attempt,
		Queue:      record.Queue,
		EnqueuedAt: record.EnqueuedAt,
		MaxRetries: record.MaxRetries,
		UniqueKey:  record.UniqueKey,
		RunAt:      record.RunAt,
		Timeout:    record.Timeout,
	}

	jobCtx := ctx
	var cancel context.CancelFunc
	if record.Timeout > 0 {
		jobCtx, cancel = context.WithTimeout(ctx, record.Timeout)
		defer cancel()
	}

	err := w.executeWithRecovery(jobCtx, meta, record)

	ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(jobCtx), defaultAckTimeout)
	defer ackCancel()

	if err == nil {
		return exec.Complete(ackCtx)
	}

	if snooze, ok := errors.AsType[*SnoozeError](err); ok {
		return exec.Snooze(ackCtx, snooze.Until())
	}

	if discard, ok := errors.AsType[*DiscardError](err); ok {
		cause := discard.Unwrap()
		if cause == nil {
			cause = discard
		}

		return exec.Discard(ackCtx, cause)
	}

	if errors.Is(err, ErrNoHandler) {
		return exec.Snooze(ackCtx, time.Now().UTC().Add(30*time.Second))
	}

	nextAttempt := record.Attempt + 1
	if record.MaxRetries >= 0 && nextAttempt > record.MaxRetries {
		return exec.Discard(ackCtx, fmt.Errorf("uppgift: max retries (%d) exceeded: %w", record.MaxRetries, err))
	}

	retryDelay := w.config.calculateBackoff(nextAttempt)
	retryAt := time.Now().UTC().Add(retryDelay)

	return exec.Fail(ackCtx, retryAt, nextAttempt, err)
}

func (w *Worker) executeWithRecovery(ctx context.Context, meta Meta, record *driver.Record) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("uppgift: panic running job kind=%q id=%q: %v\nstack:\n%s", record.Kind, record.ID, r, debug.Stack())
		}
	}()

	return w.mux.Dispatch(ctx, meta, record.Kind, record.Payload)
}
