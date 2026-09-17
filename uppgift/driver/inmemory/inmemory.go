// Package inmemory provides an in-memory driver implementation for uppgift.
//
// It is a vibe-coded implementation of the driver.Driver interface.
// This package is for local development and testing only.
// Do not use this package in production environments.
package inmemory

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/branchgrove/pogo/uppgift/driver"
)

var (
	_ driver.Driver    = (*Driver)(nil)
	_ driver.Stream    = (*stream)(nil)
	_ driver.Execution = (*execution)(nil)
)

type jobState int

const (
	statePending jobState = iota
	stateRunning
)

type storedJob struct {
	record  *driver.Record
	state   jobState
	leaseID uint64
}

// Driver stores jobs in memory.
//
// This driver is for local development and testing only.
// Do not use this driver in production environments.
type Driver struct {
	mu         sync.Mutex
	jobs       map[string]*storedJob
	uniqueKeys map[string]string
	listeners  map[*stream]chan struct{}
	nextLease  uint64
}

// New creates a new in-memory [Driver].
func New() *Driver {
	return &Driver{
		jobs:       make(map[string]*storedJob),
		uniqueKeys: make(map[string]string),
		listeners:  make(map[*stream]chan struct{}),
	}
}

func (d *Driver) wakeLocked() {
	for _, ch := range d.listeners {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (d *Driver) EnqueueBatch(ctx context.Context, msgs []*driver.Record) error {
	for _, msg := range msgs {
		err := d.Enqueue(ctx, msg)
		if err != nil {
			return err
		}
	}

	return nil
}

// Enqueue adds a job record to memory.
func (d *Driver) Enqueue(ctx context.Context, msg *driver.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if msg == nil {
		return errors.New("inmemory: nil record")
	}
	if msg.ID == "" {
		return errors.New("inmemory: empty record ID")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if msg.UniqueKey != "" {
		if _, exists := d.uniqueKeys[msg.UniqueKey]; exists {
			return nil
		}
	}

	if _, exists := d.jobs[msg.ID]; exists {
		return errors.New("inmemory: duplicate job ID")
	}

	if msg.UniqueKey != "" {
		d.uniqueKeys[msg.UniqueKey] = msg.ID
	}

	rec := *msg
	if msg.Payload != nil {
		rec.Payload = bytes.Clone(msg.Payload)
	}
	if msg.Attributes != nil {
		rec.Attributes = maps.Clone(msg.Attributes)
	}

	d.jobs[rec.ID] = &storedJob{
		record: &rec,
		state:  statePending,
	}

	d.wakeLocked()
	return nil
}

// Stream starts a stream of job executions for a worker.
func (d *Driver) Stream(ctx context.Context, cfg driver.StreamConfig) (driver.Stream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	bufSize := cfg.BufferSize
	if bufSize <= 0 {
		bufSize = 1
	}

	s := &stream{
		driver:   d,
		cfg:      cfg,
		nextCh:   make(chan driver.Execution),
		closeCh:  make(chan struct{}),
		doneCh:   make(chan struct{}),
		notifyCh: make(chan struct{}, 1),
	}

	d.mu.Lock()
	d.listeners[s] = s.notifyCh
	initBuf := make([]driver.Execution, 0, bufSize)
	nextWait, hasWait := s.fillBufferLocked(&initBuf, bufSize)
	d.mu.Unlock()

	go s.loop(ctx, initBuf, nextWait, hasWait, bufSize)

	return s, nil
}

type stream struct {
	driver   *Driver
	cfg      driver.StreamConfig
	nextCh   chan driver.Execution
	closeCh  chan struct{}
	doneCh   chan struct{}
	notifyCh chan struct{}
	errMu    sync.Mutex
	err      error
	once     sync.Once
}

// Next returns the channel for job executions.
func (s *stream) Next() <-chan driver.Execution {
	return s.nextCh
}

// Err returns the error that caused the stream to stop.
func (s *stream) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

// Close stops the stream and closes the execution channel.
func (s *stream) Close() error {
	s.once.Do(func() {
		close(s.closeCh)
		<-s.doneCh
	})
	return nil
}

func (s *stream) setErr(err error) {
	s.errMu.Lock()
	s.err = err
	s.errMu.Unlock()
}

func (s *stream) fillBufferLocked(buf *[]driver.Execution, max int) (time.Duration, bool) {
	var (
		minWait time.Duration
		hasWait bool
	)
	for len(*buf) < max {
		exec, wait, ok := s.nextExecutionLocked()
		if exec != nil {
			*buf = append(*buf, exec)
			continue
		}
		if ok && (!hasWait || wait < minWait) {
			minWait = wait
			hasWait = true
		}
		break
	}
	if !hasWait {
		now := time.Now().UTC()
		for _, job := range s.driver.jobs {
			if job.state != statePending {
				continue
			}
			if len(s.cfg.Queues) > 0 && !slices.Contains(s.cfg.Queues, job.record.Queue) {
				continue
			}
			if job.record.RunAt.After(now) {
				wait := job.record.RunAt.Sub(now)
				if !hasWait || wait < minWait {
					minWait = wait
					hasWait = true
				}
			}
		}
	}
	return minWait, hasWait
}

func (s *stream) loop(ctx context.Context, initBuf []driver.Execution, initWait time.Duration, initHasWait bool, bufSize int) {
	buffer := initBuf
	defer func() {
		s.driver.mu.Lock()
		delete(s.driver.listeners, s)
		for _, exec := range buffer {
			if e, ok := exec.(*execution); ok {
				if job, exists := s.driver.jobs[e.record.ID]; exists && job.state == stateRunning && job.leaseID == e.leaseID {
					job.state = statePending
					job.leaseID = 0
				}
			}
		}
		s.driver.wakeLocked()
		s.driver.mu.Unlock()

		close(s.nextCh)
		close(s.doneCh)
	}()

	timer := time.NewTimer(0)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	defer timer.Stop()

	nextWait := initWait
	hasWait := initHasWait

	for {
		if hasWait {
			if nextWait <= 0 {
				nextWait = time.Microsecond
			}
			timer.Reset(nextWait)
		}

		var sendCh chan driver.Execution
		var curExec driver.Execution
		if len(buffer) > 0 {
			sendCh = s.nextCh
			curExec = buffer[0]
		}

		select {
		case <-ctx.Done():
			s.setErr(ctx.Err())
			return
		case <-s.closeCh:
			return
		case sendCh <- curExec:
			buffer = buffer[1:]
			s.driver.mu.Lock()
			w, hw := s.fillBufferLocked(&buffer, bufSize)
			s.driver.mu.Unlock()
			nextWait, hasWait = w, hw

		case <-s.notifyCh:
			s.driver.mu.Lock()
			w, hw := s.fillBufferLocked(&buffer, bufSize)
			s.driver.mu.Unlock()
			nextWait, hasWait = w, hw

		case <-timer.C:
			s.driver.mu.Lock()
			w, hw := s.fillBufferLocked(&buffer, bufSize)
			s.driver.mu.Unlock()
			nextWait, hasWait = w, hw
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

func (s *stream) nextExecutionLocked() (driver.Execution, time.Duration, bool) {
	now := time.Now().UTC()
	var (
		bestJob  *storedJob
		nextWait time.Duration
		hasWait  bool
	)

	for _, job := range s.driver.jobs {
		if job.state != statePending {
			continue
		}
		if len(s.cfg.Queues) > 0 && !slices.Contains(s.cfg.Queues, job.record.Queue) {
			continue
		}

		if !job.record.RunAt.After(now) {
			if bestJob == nil ||
				job.record.RunAt.Before(bestJob.record.RunAt) ||
				(job.record.RunAt.Equal(bestJob.record.RunAt) && (job.record.EnqueuedAt.Before(bestJob.record.EnqueuedAt) || (job.record.EnqueuedAt.Equal(bestJob.record.EnqueuedAt) && job.record.ID < bestJob.record.ID))) {
				bestJob = job
			}
		} else {
			wait := job.record.RunAt.Sub(now)
			if !hasWait || wait < nextWait {
				nextWait = wait
				hasWait = true
			}
		}
	}

	if bestJob != nil {
		bestJob.state = stateRunning
		s.driver.nextLease++
		bestJob.leaseID = s.driver.nextLease
		recCopy := *bestJob.record
		return &execution{
			driver:  s.driver,
			record:  &recCopy,
			leaseID: bestJob.leaseID,
		}, 0, false
	}

	return nil, nextWait, hasWait
}

type execution struct {
	driver  *Driver
	record  *driver.Record
	leaseID uint64
}

// Record returns the job record for this execution.
func (e *execution) Record() *driver.Record {
	return e.record
}

// Complete marks the execution as finished and deletes the job.
func (e *execution) Complete(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	e.driver.mu.Lock()
	defer e.driver.mu.Unlock()

	job, ok := e.driver.jobs[e.record.ID]
	if !ok || job.state != stateRunning || job.leaseID != e.leaseID {
		return nil
	}

	delete(e.driver.jobs, e.record.ID)
	if e.record.UniqueKey != "" && e.driver.uniqueKeys[e.record.UniqueKey] == e.record.ID {
		delete(e.driver.uniqueKeys, e.record.UniqueKey)
	}

	return nil
}

// Snooze postpones the job until the specified time.
func (e *execution) Snooze(ctx context.Context, until time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	e.driver.mu.Lock()
	defer e.driver.mu.Unlock()

	job, ok := e.driver.jobs[e.record.ID]
	if !ok || job.state != stateRunning || job.leaseID != e.leaseID {
		return nil
	}

	job.record.RunAt = until.UTC()
	job.state = statePending
	job.leaseID = 0

	e.driver.wakeLocked()
	return nil
}

// Fail records a failure and reschedules the job for retry.
func (e *execution) Fail(ctx context.Context, retryAt time.Time, attempt int, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	e.driver.mu.Lock()
	defer e.driver.mu.Unlock()

	job, ok := e.driver.jobs[e.record.ID]
	if !ok || job.state != stateRunning || job.leaseID != e.leaseID {
		return nil
	}

	job.record.RunAt = retryAt.UTC()
	job.record.Attempt = attempt
	job.state = statePending
	job.leaseID = 0

	e.driver.wakeLocked()
	return nil
}

// Discard unschedules the job.
func (e *execution) Discard(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	e.driver.mu.Lock()
	defer e.driver.mu.Unlock()

	job, ok := e.driver.jobs[e.record.ID]
	if !ok || job.state != stateRunning || job.leaseID != e.leaseID {
		return nil
	}

	delete(e.driver.jobs, e.record.ID)
	if e.record.UniqueKey != "" && e.driver.uniqueKeys[e.record.UniqueKey] == e.record.ID {
		delete(e.driver.uniqueKeys, e.record.UniqueKey)
	}

	return nil
}
