package pgx

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/branchgrove/pogo/uppgift/driver"
	"github.com/branchgrove/pogo/uppgift/driver/pgx/internal/sqlc"
	"github.com/jackc/pgx/v5/pgtype"
)

var _ driver.Execution = (*execution)(nil)

type execution struct {
	record *driver.Record
	stream *stream
	once   sync.Once
}

func (e *execution) finish() {
	e.once.Do(func() {
		e.stream.execDoneCh <- struct{}{}
	})
}

// Complete implements [driver.Execution].
func (e *execution) Complete(ctx context.Context) error {
	defer e.finish()

	q := e.stream.driver.queries
	if tx, ok := TxFromContext(ctx); ok {
		q = q.WithTx(tx)
	}

	rows, err := q.CompleteJob(ctx, sqlc.CompleteJobParams{
		ID:       e.record.ID,
		LockedBy: pgtype.Text{String: e.stream.cfg.WorkerID, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("pgx: complete job: %w", err)
	}

	if rows == 0 {
		return driver.ErrLeaseLost
	}

	return nil
}

// Discard implements [driver.Execution].
func (e *execution) Discard(ctx context.Context, jobErr error) error {
	defer e.finish()

	var lastErr pgtype.Text
	if jobErr != nil {
		lastErr = pgtype.Text{String: jobErr.Error(), Valid: true}
	}

	q := e.stream.driver.queries
	if tx, ok := TxFromContext(ctx); ok {
		q = q.WithTx(tx)
	}

	rows, err := q.DiscardJob(ctx, sqlc.DiscardJobParams{
		ID:        e.record.ID,
		LockedBy:  pgtype.Text{String: e.stream.cfg.WorkerID, Valid: true},
		LastError: lastErr,
	})
	if err != nil {
		return fmt.Errorf("pgx: discard job: %w", err)
	}

	if rows == 0 {
		return driver.ErrLeaseLost
	}

	return nil
}

// Fail implements [driver.Execution].
func (e *execution) Fail(ctx context.Context, retryAt time.Time, attempt int, jobErr error) error {
	defer e.finish()

	var lastErr pgtype.Text
	if jobErr != nil {
		lastErr = pgtype.Text{String: jobErr.Error(), Valid: true}
	}

	tx, ctxHasTx := TxFromContext(ctx)
	var err error
	if !ctxHasTx {
		tx, err = e.stream.driver.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("pgx: begin tx: %w", err)
		}
		defer tx.Rollback(ctx)
	}

	qtx := e.stream.driver.queries.WithTx(tx)

	rows, err := qtx.FailJob(ctx, sqlc.FailJobParams{
		ID:        e.record.ID,
		LockedBy:  pgtype.Text{String: e.stream.cfg.WorkerID, Valid: true},
		RunAt:     retryAt,
		Attempt:   int32(attempt),
		LastError: lastErr,
	})
	if err != nil {
		return fmt.Errorf("pgx: fail job: %w", err)
	}

	if rows == 0 {
		return driver.ErrLeaseLost
	}

	if err := qtx.NotifyQueue(ctx, e.record.Queue); err != nil {
		return fmt.Errorf("pgx: notify queue %q: %w", e.record.Queue, err)
	}

	if !ctxHasTx {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("pgx: commit tx: %w", err)
		}
	}

	e.record.RunAt = retryAt
	e.record.Attempt = attempt
	return nil
}

// Record implements [driver.Execution].
func (e *execution) Record() *driver.Record {
	return e.record
}

// Snooze implements [driver.Execution].
func (e *execution) Snooze(ctx context.Context, until time.Time) error {
	defer e.finish()

	tx, ctxHasTx := TxFromContext(ctx)
	var err error
	if !ctxHasTx {
		tx, err = e.stream.driver.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("pgx: begin tx: %w", err)
		}
		defer tx.Rollback(ctx)
	}

	qtx := e.stream.driver.queries.WithTx(tx)

	rows, err := qtx.SnoozeJob(ctx, sqlc.SnoozeJobParams{
		ID:       e.record.ID,
		LockedBy: pgtype.Text{String: e.stream.cfg.WorkerID, Valid: true},
		RunAt:    until,
	})
	if err != nil {
		return fmt.Errorf("pgx: snooze job: %w", err)
	}

	if rows == 0 {
		return driver.ErrLeaseLost
	}

	if err := qtx.NotifyQueue(ctx, e.record.Queue); err != nil {
		return fmt.Errorf("pgx: notify queue %q: %w", e.record.Queue, err)
	}

	if !ctxHasTx {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("pgx: commit tx: %w", err)
		}
	}

	e.record.RunAt = until
	return nil
}
