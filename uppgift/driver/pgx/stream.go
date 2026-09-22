package pgx

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/branchgrove/pogo/uppgift/driver"
	"github.com/branchgrove/pogo/uppgift/driver/pgx/internal/sqlc"
	"github.com/branchgrove/pogo/uppgift/driver/pgx/internal/types"
)

var _ driver.Stream = (*stream)(nil)

type stream struct {
	driver     *Driver
	cfg        driver.StreamConfig
	nextCh     chan driver.Execution
	closeCh    chan struct{}
	doneCh     chan struct{}
	notifyCh   chan struct{}
	execDoneCh chan struct{}
	cancel     context.CancelFunc

	errMu sync.Mutex
	err   error
	once  sync.Once
}

// Close stops the stream and releases any un-dispatched in-memory buffer jobs back to PostgreSQL.
func (s *stream) Close() error {
	s.once.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		close(s.closeCh)
		<-s.doneCh
	})
	return nil
}

// Err returns the error that stopped the stream.
func (s *stream) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

// Next returns the channel delivering claimed job executions.
func (s *stream) Next() <-chan driver.Execution {
	return s.nextCh
}

func (s *stream) setErr(err error) {
	s.errMu.Lock()
	s.err = err
	s.errMu.Unlock()
}

// notify sends a non-blocking wake-up signal to the stream event loop.
func (s *stream) notify() {
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

// listen maintains a dedicated LISTEN connection to receive pg_notify queue events.
func (s *stream) listen(ctx context.Context) {
	// TODO: the backoff needs some jitter
	backoff := 100 * time.Millisecond
	maxBackoff := 2 * time.Second

	for {
		// TODO: isn't an actual error just swallowed here? can't acquire fail on e.g bad password and such? Is the pool really valid when instantiated i.e connect/check password and network?
		conn, err := s.driver.pool.Acquire(ctx)
		if err != nil {
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
				continue
			case <-ctx.Done():
				return
			}
		}

		// TODO: same thing here, isn't a failure swallowed meaning not observable?
		_, err = conn.Exec(ctx, "LISTEN uppgift_events;")
		if err != nil {
			conn.Release()
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
				continue
			case <-ctx.Done():
				return
			}
		}

		// Successfully connected and listening; reset backoff
		backoff = 100 * time.Millisecond

		// Signal notify channel to ensure any jobs enqueued during reconnect are processed
		s.notify()

		for {
			// TODO: same thing here err is not observable
			notification, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				conn.Release()
				break // Reconnect in outer loop
			}

			// Filter notifications by configured queues (empty cfg.Queues monitors all queues)
			if len(s.cfg.Queues) == 0 || slices.Contains(s.cfg.Queues, notification.Payload) {
				s.notify()
			}
		}
	}
}

// loop runs the stream event loop coordinating capacity, job claiming, dynamic wake timers, and dispatching.
func (s *stream) loop(ctx context.Context) {
	var buffer []*execution
	// Currently running jobs
	inFlight := 0
	// Is true if there are more jobs in postgres that should be immediately queried
	hasPendingWork := true

	defer func() {
		// Graceful shutdown: release un-dispatched jobs held in memory back to 'available' state
		if len(buffer) > 0 {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()

			ids := make([]string, len(buffer))
			queues := make(map[string]struct{})
			for i, exec := range buffer {
				ids[i] = exec.record.ID
				queues[exec.record.Queue] = struct{}{}
			}

			// TODO: capture error and make it observable
			_ = s.driver.queries.ReleaseBuffer(cleanupCtx, sqlc.ReleaseBufferParams{
				Ids:      ids,
				WorkerID: s.cfg.WorkerID,
			})

			for q := range queues {
				// TODO: capture error and make it observable
				_ = s.driver.queries.NotifyQueue(cleanupCtx, q)
			}
		}

		close(s.nextCh)
		close(s.doneCh)
	}()

	// Initialize an idle timer that is immediately stopped and drained.
	timer := time.NewTimer(0)
	// Timer already expired and put a value in timer.C
	if !timer.Stop() {
		select {
		// Pull the stale value out (drain it)
		case <-timer.C:
		// If nothing was in the channel, don't block
		default:
		}
	}
	defer timer.Stop()

	for {
		// Capacity-aware batch claim
		freeSlots := s.cfg.BufferSize - inFlight - len(buffer)
		if freeSlots > 0 && hasPendingWork {
			// TODO: any actual error is silently dropped
			claimed, nextRunAt, hasNext, err := s.claim(ctx, freeSlots)
			if err == nil {
				buffer = append(buffer, claimed...)
				if len(claimed) == freeSlots {
					hasPendingWork = true
				} else {
					hasPendingWork = false
				}

				if hasNext {
					wait := time.Until(nextRunAt)
					if wait <= 0 {
						wait = time.Microsecond
					}
					timer.Reset(wait)
				}
			}
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
			inFlight++

		case <-s.execDoneCh:
			if inFlight > 0 {
				inFlight--
			}

		case <-s.notifyCh:
			hasPendingWork = true
			// Short randomized jitter (0-25ms) on NOTIFY to prevent stampedes across peer workers
			jitter := rand.N(25 * time.Millisecond)
			select {
			case <-time.After(jitter):
			case <-ctx.Done():
				s.setErr(ctx.Err())
				return
			case <-s.closeCh:
				return
			}

		case <-timer.C:
			hasPendingWork = true
		}

		// Drain timer channel if stopped
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

// claim executes the atomic CTE batch claim and next-wake discovery query.
func (s *stream) claim(ctx context.Context, freeSlots int) ([]*execution, time.Time, bool, error) {
	rows, err := s.driver.queries.ClaimJobs(ctx, sqlc.ClaimJobsParams{
		Queues:     s.cfg.Queues,
		ClaimLimit: int32(freeSlots),
		WorkerID:   s.cfg.WorkerID,
	})
	if err != nil {
		return nil, time.Time{}, false, fmt.Errorf("pgx: claim jobs: %w", err)
	}

	var claimed []*execution
	var nextRunAt time.Time
	var hasNext bool

	for _, row := range rows {
		if row.NextRunAt.Valid && !row.NextRunAt.Time.IsZero() {
			nextRunAt = row.NextRunAt.Time
			hasNext = true
		}

		if !row.ID.Valid || row.ID.String == "" {
			continue
		}

		var attrs types.Attributes
		if err := attrs.ScanBytes(row.Attributes); err != nil {
			attrs = make(types.Attributes)
		}

		uniqueKey := ""
		if row.UniqueKey.Valid {
			uniqueKey = row.UniqueKey.String
		}

		// Construct driver.Record. Note: Record is a newly allocated value owned by the caller/worker.
		rec := &driver.Record{
			ID:         row.ID.String,
			Kind:       row.Kind.String,
			Queue:      row.Queue.String,
			Payload:    row.Payload,
			Attributes: attrs,
			MaxRetries: int(row.MaxRetries.Int32),
			Attempt:    int(row.Attempt.Int32),
			Timeout:    time.Duration(row.TimeoutMs.Int64) * time.Millisecond,
			RunAt:      row.RunAt.Time,
			EnqueuedAt: row.EnqueuedAt.Time,
			UniqueKey:  uniqueKey,
		}

		exec := &execution{
			record: rec,
			stream: s,
		}
		claimed = append(claimed, exec)
	}

	return claimed, nextRunAt, hasNext, nil
}
