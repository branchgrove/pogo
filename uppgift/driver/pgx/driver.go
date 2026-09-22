package pgx

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/branchgrove/pogo/uppgift/driver"
	"github.com/branchgrove/pogo/uppgift/driver/pgx/internal/sqlc"
	"github.com/branchgrove/pogo/uppgift/driver/pgx/internal/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxstdlib "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	goosedb "github.com/pressly/goose/v3/database"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

var _ driver.Driver = (*Driver)(nil)

func NewDriver(pool *pgxpool.Pool) *Driver {
	return &Driver{
		pool:    pool,
		queries: sqlc.New(pool),
	}
}

type Driver struct {
	pool    *pgxpool.Pool
	queries *sqlc.Queries
}

type txContextKey struct{}

// WithTx returns a new context carrying the given pgx.Tx.
func WithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txContextKey{}, tx)
}

// TxFromContext retrieves a pgx.Tx from the context if present.
func TxFromContext(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txContextKey{}).(pgx.Tx)
	return tx, ok
}

func (d *Driver) Ping(ctx context.Context) error {
	return d.pool.Ping(ctx)
}

func (d *Driver) Migrate(ctx context.Context) error {
	dbConn := pgxstdlib.OpenDBFromPool(d.pool)
	defer dbConn.Close()

	if _, err := dbConn.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS uppgift;"); err != nil {
		return err
	}

	store, err := goosedb.NewStore(goosedb.DialectPostgres, "uppgift.schema_migrations")
	if err != nil {
		return err
	}

	subFS, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return err
	}

	provider, err := goose.NewProvider(goose.DialectCustom, dbConn, subFS, goose.WithStore(store))
	if err != nil {
		return err
	}

	_, err = provider.Up(ctx)
	return err
}

// Enqueue implements [driver.Driver].
func (d *Driver) Enqueue(ctx context.Context, msg *driver.Record) error {
	return d.EnqueueBatch(ctx, []*driver.Record{msg})
}

// EnqueueBatch implements [driver.Driver].
func (d *Driver) EnqueueBatch(ctx context.Context, msgs []*driver.Record) error {
	if len(msgs) == 0 {
		return nil
	}

	n := len(msgs)
	params := sqlc.EnqueueJobsParams{
		ID:         make([]string, n),
		Queue:      make([]string, n),
		Kind:       make([]string, n),
		Payload:    make([][]byte, n),
		Attributes: make([][]byte, n),
		MaxRetries: make([]int32, n),
		RunAt:      make([]pgtype.Timestamptz, n),
		TimeoutMs:  make([]int64, n),
		UniqueKey:  make([]string, n),
		EnqueuedAt: make([]pgtype.Timestamptz, n),
	}

	distinctQueues := make(map[string]struct{})

	for i, msg := range msgs {
		payload := msg.Payload
		if payload == nil {
			payload = []byte{}
		}

		attrsBytes, err := types.Attributes(msg.Attributes).BytesValue()
		if err != nil {
			return fmt.Errorf("pgx: serialize attributes: %w", err)
		}

		params.ID[i] = msg.ID
		params.Queue[i] = msg.Queue
		params.Kind[i] = msg.Kind
		params.Payload[i] = payload
		params.Attributes[i] = attrsBytes
		params.MaxRetries[i] = int32(msg.MaxRetries)
		params.RunAt[i] = pgtype.Timestamptz{Time: msg.RunAt, Valid: true}
		params.TimeoutMs[i] = msg.Timeout.Milliseconds()
		params.UniqueKey[i] = msg.UniqueKey
		params.EnqueuedAt[i] = pgtype.Timestamptz{Time: msg.EnqueuedAt, Valid: true}

		distinctQueues[msg.Queue] = struct{}{}
	}

	var err error
	tx, ctxHasTx := TxFromContext(ctx)

	if !ctxHasTx {
		tx, err = d.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("pgx: begin tx: %w", err)
		}
		defer tx.Rollback(ctx)
	}

	qtx := d.queries.WithTx(tx)

	if err := qtx.EnqueueJobs(ctx, params); err != nil {
		return fmt.Errorf("pgx: enqueue jobs: %w", err)
	}

	for q := range distinctQueues {
		if err := qtx.NotifyQueue(ctx, q); err != nil {
			return fmt.Errorf("pgx: notify queue %q: %w", q, err)
		}
	}

	if !ctxHasTx {
		err = tx.Commit(ctx)
		if err != nil {
			return fmt.Errorf("pgx: commit tx: %w", err)
		}

		return nil
	}

	return nil
}

// PurgeDiscarded deletes dead-letter jobs in state='discarded' older than the specified duration.
func (d *Driver) PurgeDiscarded(ctx context.Context, olderThan time.Duration) (int64, error) {
	before := time.Now().UTC().Add(-olderThan)
	return d.queries.PurgeDiscardedJobs(ctx, pgtype.Timestamptz{Time: before, Valid: true})
}

// Stream implements [driver.Driver].
//
// Assumptions and caller contracts:
//   - cfg.BufferSize determines maximum active concurrency. The stream enforces that
//     total leased jobs (inFlight + buffered) never exceeds cfg.BufferSize.
//   - cfg.WorkerID uniquely identifies the worker holding claimed job leases.
//   - Executions delivered through Next() are owned by the receiving worker goroutine.
//     Each execution should have exactly one lifecycle terminal call (Complete, Snooze, Fail, or Discard).
func (d *Driver) Stream(ctx context.Context, cfg driver.StreamConfig) (driver.Stream, error) {
	if cfg.WorkerID == "" {
		return nil, errors.New("pgx: worker ID is required")
	}
	if cfg.BufferSize <= 0 {
		return nil, errors.New("pgx: buffer size must be greater than zero")
	}

	streamCtx, cancel := context.WithCancel(ctx)
	s := &stream{
		driver:     d,
		cfg:        cfg,
		nextCh:     make(chan driver.Execution),
		closeCh:    make(chan struct{}),
		doneCh:     make(chan struct{}),
		notifyCh:   make(chan struct{}, 1),
		execDoneCh: make(chan struct{}, cfg.BufferSize),
		cancel:     cancel,
	}

	go s.listen(streamCtx)
	go s.loop(streamCtx)

	return s, nil
}
