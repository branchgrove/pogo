package pgx

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/branchgrove/pogo/uppgift"
	"github.com/branchgrove/pogo/uppgift/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrationsEmbedded(t *testing.T) {
	subFS, err := fs.Sub(migrationsFS, "migrations")
	require.NoError(t, err)

	store, err := database.NewStore(database.DialectPostgres, "uppgift.schema_migrations")
	require.NoError(t, err)

	db, err := sql.Open("pgx", "")
	require.NoError(t, err)
	defer db.Close()

	provider, err := goose.NewProvider(goose.DialectCustom, db, subFS, goose.WithStore(store))
	require.NoError(t, err)

	sources := provider.ListSources()
	require.Len(t, sources, 1)
	require.Equal(t, int64(1), sources[0].Version)
	require.Equal(t, "0001_init.sql", sources[0].Path)
}

func getTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	connStr := os.Getenv("TEST_DATABASE_URL")
	if connStr == "" {
		// Default to local test postgres if running
		connStr = "postgres://postgres:postgres@localhost:5433/uppgift_test?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		t.Skipf("skipping pgx integration tests: %v", err)
		return nil
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Skipf("skipping pgx integration tests: %v", err)
		return nil
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		// Try fallback 5432
		if connStr == "postgres://postgres:postgres@localhost:5433/uppgift_test?sslmode=disable" {
			fallbackStr := "postgres://postgres:postgres@localhost:5432/uppgift_test?sslmode=disable"
			if fallbackCfg, fbErr := pgxpool.ParseConfig(fallbackStr); fbErr == nil {
				if fbPool, pErr := pgxpool.NewWithConfig(ctx, fallbackCfg); pErr == nil {
					if pingErr := fbPool.Ping(ctx); pingErr == nil {
						t.Cleanup(func() { fbPool.Close() })
						return fbPool
					}
					fbPool.Close()
				}
			}
		}
		t.Skipf("skipping pgx integration tests: cannot connect to postgres at %s: %v", connStr, err)
		return nil
	}

	t.Cleanup(func() {
		pool.Close()
	})

	return pool
}

func setupTestDriver(t *testing.T) (*Driver, *pgxpool.Pool) {
	t.Helper()
	pool := getTestPool(t)
	d := NewDriver(pool)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS uppgift CASCADE;")

	err := d.Migrate(ctx)
	require.NoError(t, err)

	return d, pool
}

func TestEnqueue(t *testing.T) {
	d, pool := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Listen for notifications
	listenerConn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer listenerConn.Release()

	_, err = listenerConn.Exec(ctx, "LISTEN uppgift_events;")
	require.NoError(t, err)

	jobID := uuid.New().String()
	now := time.Now().UTC().Truncate(time.Microsecond)
	runAt := now.Add(10 * time.Minute)

	rec := &driver.Record{
		ID:         jobID,
		Kind:       "send_email",
		Queue:      "emails",
		Payload:    []byte(`{"email":"user@example.com"}`),
		Attributes: map[string]string{"env": "test", "priority": "high"},
		MaxRetries: 10,
		Timeout:    2 * time.Minute,
		RunAt:      runAt,
		EnqueuedAt: now,
		UniqueKey:  "email:user@example.com",
	}

	err = d.Enqueue(ctx, rec)
	require.NoError(t, err)

	// Check notification received
	notifCtx, notifCancel := context.WithTimeout(ctx, 2*time.Second)
	defer notifCancel()
	notification, err := listenerConn.Conn().WaitForNotification(notifCtx)
	require.NoError(t, err)
	assert.Equal(t, "uppgift_events", notification.Channel)
	assert.Equal(t, "emails", notification.Payload)

	// Query DB directly to verify all stored columns
	var (
		id, queue, kind, state, uniqueKey string
		payload, attributes               []byte
		attempt, maxRetries               int
		timeoutMs                         int64
		dbRunAt, dbEnqueuedAt             time.Time
	)

	row := pool.QueryRow(ctx, `
		SELECT id, queue, kind, payload, attributes, state, attempt, max_retries, run_at, timeout_ms, unique_key, enqueued_at
		FROM uppgift.jobs
		WHERE id = $1
	`, jobID)

	err = row.Scan(&id, &queue, &kind, &payload, &attributes, &state, &attempt, &maxRetries, &dbRunAt, &timeoutMs, &uniqueKey, &dbEnqueuedAt)
	require.NoError(t, err)

	assert.Equal(t, jobID, id)
	assert.Equal(t, "emails", queue)
	assert.Equal(t, "send_email", kind)
	assert.Equal(t, []byte(`{"email":"user@example.com"}`), payload)
	assert.JSONEq(t, `{"env":"test","priority":"high"}`, string(attributes))
	assert.Equal(t, "available", state)
	assert.Equal(t, 0, attempt)
	assert.Equal(t, 10, maxRetries)
	assert.Equal(t, int64(120000), timeoutMs)
	assert.Equal(t, "email:user@example.com", uniqueKey)
	assert.True(t, runAt.Equal(dbRunAt.UTC()))
	assert.True(t, now.Equal(dbEnqueuedAt.UTC()))
}

func TestEnqueueUniqueKeyDeduplication(t *testing.T) {
	d, _ := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rec1 := &driver.Record{
		ID:        uuid.New().String(),
		Kind:      "sync_data",
		Queue:     "default",
		UniqueKey: "unique:sync:1",
	}

	err := d.Enqueue(ctx, rec1)
	require.NoError(t, err)

	// Enqueue duplicate unique key while rec1 is available
	rec2 := &driver.Record{
		ID:        uuid.New().String(),
		Kind:      "sync_data",
		Queue:     "default",
		UniqueKey: "unique:sync:1",
	}

	err = d.Enqueue(ctx, rec2)
	require.NoError(t, err) // Should succeed via DO NOTHING

	// Without unique key (empty string), multiple records are allowed
	rec3 := &driver.Record{
		ID:    uuid.New().String(),
		Kind:  "sync_data",
		Queue: "default",
	}
	rec4 := &driver.Record{
		ID:    uuid.New().String(),
		Kind:  "sync_data",
		Queue: "default",
	}

	require.NoError(t, d.Enqueue(ctx, rec3))
	require.NoError(t, d.Enqueue(ctx, rec4))
}

func TestEnqueueBatch(t *testing.T) {
	d, pool := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Empty batch should be no-op
	require.NoError(t, d.EnqueueBatch(ctx, nil))
	require.NoError(t, d.EnqueueBatch(ctx, []*driver.Record{}))

	// Listen for notifications
	listenerConn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer listenerConn.Release()

	_, err = listenerConn.Exec(ctx, "LISTEN uppgift_events;")
	require.NoError(t, err)

	id1 := uuid.New().String()
	id2 := uuid.New().String()
	id3 := uuid.New().String()

	records := []*driver.Record{
		{
			ID:        id1,
			Kind:      "task_a",
			Queue:     "queue_one",
			UniqueKey: "key:1",
		},
		{
			ID:        id2,
			Kind:      "task_b",
			Queue:     "queue_two",
			UniqueKey: "key:2",
		},
		{
			ID:        id3,
			Kind:      "task_c",
			Queue:     "queue_one",
			UniqueKey: "key:1", // Duplicate within same queue / batch with key:1
		},
	}

	err = d.EnqueueBatch(ctx, records)
	require.NoError(t, err)

	// Notifications should be received for queue_one and queue_two
	receivedQueues := make(map[string]bool)
	for len(receivedQueues) < 2 {
		notifCtx, notifCancel := context.WithTimeout(ctx, 2*time.Second)
		notification, err := listenerConn.Conn().WaitForNotification(notifCtx)
		notifCancel()
		require.NoError(t, err)
		receivedQueues[notification.Payload] = true
	}

	assert.True(t, receivedQueues["queue_one"])
	assert.True(t, receivedQueues["queue_two"])

	// Verify only 2 jobs exist because id3 had duplicate unique_key "key:1"
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM uppgift.jobs;").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func setupRunningExecution(t *testing.T, d *Driver, pool *pgxpool.Pool, workerID string) (*execution, *stream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID := uuid.New().String()
	now := time.Now().UTC().Truncate(time.Microsecond)
	rec := &driver.Record{
		ID:         jobID,
		Kind:       "test_task",
		Queue:      "default",
		Payload:    []byte(`{"data":"123"}`),
		MaxRetries: 3,
		Timeout:    1 * time.Minute,
		RunAt:      now,
		EnqueuedAt: now,
	}

	err := d.Enqueue(ctx, rec)
	require.NoError(t, err)

	// Lock the job to state='running'
	_, err = pool.Exec(ctx, `
	UPDATE uppgift.jobs
	SET state = 'running',
	    locked_by = $1,
	    locked_until = $2
	WHERE id = $3
`, workerID, now.Add(5*time.Minute), jobID)
	require.NoError(t, err)

	st := &stream{
		driver: d,
		cfg: driver.StreamConfig{
			WorkerID: workerID,
		},
		execDoneCh: make(chan struct{}, 10),
	}

	exec := &execution{
		record: rec,
		stream: st,
	}

	return exec, st
}

func TestExecution_Complete(t *testing.T) {
	d, pool := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	workerID := "worker-test-complete"
	exec, st := setupRunningExecution(t, d, pool, workerID)

	assert.Equal(t, exec.record.ID, exec.Record().ID)

	err := exec.Complete(ctx)
	require.NoError(t, err)

	// Verify job was deleted
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM uppgift.jobs WHERE id = $1;", exec.record.ID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Verify finish() was called and sent to execDoneCh
	select {
	case <-st.execDoneCh:
	default:
		t.Fatal("expected message on execDoneCh")
	}

	// Completing again should return ErrLeaseLost
	err = exec.Complete(ctx)
	require.ErrorIs(t, err, driver.ErrLeaseLost)
}

func TestExecution_Snooze(t *testing.T) {
	d, pool := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Listen for notifications
	listenerConn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer listenerConn.Release()

	_, err = listenerConn.Exec(ctx, "LISTEN uppgift_events;")
	require.NoError(t, err)

	workerID := "worker-test-snooze"
	exec, st := setupRunningExecution(t, d, pool, workerID)

	until := time.Now().UTC().Add(10 * time.Minute).Truncate(time.Microsecond)
	err = exec.Snooze(ctx, until)
	require.NoError(t, err)

	// Verify notification received
	notifCtx, notifCancel := context.WithTimeout(ctx, 2*time.Second)
	defer notifCancel()
	notification, err := listenerConn.Conn().WaitForNotification(notifCtx)
	require.NoError(t, err)
	assert.Equal(t, "uppgift_events", notification.Channel)
	assert.Equal(t, "default", notification.Payload)

	// Verify DB state
	var (
		state     string
		dbRunAt   time.Time
		lockedBy  *string
		lockedUnt *time.Time
	)
	err = pool.QueryRow(ctx, `
	SELECT state, run_at, locked_by, locked_until
	FROM uppgift.jobs
	WHERE id = $1
`, exec.record.ID).Scan(&state, &dbRunAt, &lockedBy, &lockedUnt)
	require.NoError(t, err)

	assert.Equal(t, "available", state)
	assert.True(t, until.Equal(dbRunAt.UTC()))
	assert.Nil(t, lockedBy)
	assert.Nil(t, lockedUnt)
	assert.True(t, until.Equal(exec.Record().RunAt))

	// Verify finish() signaled
	select {
	case <-st.execDoneCh:
	default:
		t.Fatal("expected message on execDoneCh")
	}

	// Snoozing again when not running should return ErrLeaseLost
	err = exec.Snooze(ctx, until)
	require.ErrorIs(t, err, driver.ErrLeaseLost)
}

func TestExecution_Fail(t *testing.T) {
	d, pool := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Listen for notifications
	listenerConn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer listenerConn.Release()

	_, err = listenerConn.Exec(ctx, "LISTEN uppgift_events;")
	require.NoError(t, err)

	workerID := "worker-test-fail"
	exec, st := setupRunningExecution(t, d, pool, workerID)

	retryAt := time.Now().UTC().Add(30 * time.Second).Truncate(time.Microsecond)
	testErr := assert.AnError

	err = exec.Fail(ctx, retryAt, 2, testErr)
	require.NoError(t, err)

	// Verify notification received
	notifCtx, notifCancel := context.WithTimeout(ctx, 2*time.Second)
	defer notifCancel()
	notification, err := listenerConn.Conn().WaitForNotification(notifCtx)
	require.NoError(t, err)
	assert.Equal(t, "uppgift_events", notification.Channel)
	assert.Equal(t, "default", notification.Payload)

	// Verify DB state
	var (
		state     string
		attempt   int
		lastError *string
		dbRunAt   time.Time
		lockedBy  *string
		lockedUnt *time.Time
	)
	err = pool.QueryRow(ctx, `
	SELECT state, attempt, last_error, run_at, locked_by, locked_until
	FROM uppgift.jobs
	WHERE id = $1
`, exec.record.ID).Scan(&state, &attempt, &lastError, &dbRunAt, &lockedBy, &lockedUnt)
	require.NoError(t, err)

	assert.Equal(t, "available", state)
	assert.Equal(t, 2, attempt)
	require.NotNil(t, lastError)
	assert.Equal(t, testErr.Error(), *lastError)
	assert.True(t, retryAt.Equal(dbRunAt.UTC()))
	assert.Nil(t, lockedBy)
	assert.Nil(t, lockedUnt)
	assert.Equal(t, 2, exec.Record().Attempt)
	assert.True(t, retryAt.Equal(exec.Record().RunAt))

	// Verify finish() signaled
	select {
	case <-st.execDoneCh:
	default:
		t.Fatal("expected message on execDoneCh")
	}

	// Failing again when not running should return ErrLeaseLost
	err = exec.Fail(ctx, retryAt, 3, testErr)
	require.ErrorIs(t, err, driver.ErrLeaseLost)
}

func TestExecution_Discard(t *testing.T) {
	d, pool := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	workerID := "worker-test-discard"
	exec, st := setupRunningExecution(t, d, pool, workerID)

	testErr := assert.AnError
	err := exec.Discard(ctx, testErr)
	require.NoError(t, err)

	// Verify DB state is discarded
	var (
		state     string
		lastError *string
		lockedBy  *string
		lockedUnt *time.Time
	)
	err = pool.QueryRow(ctx, `
	SELECT state, last_error, locked_by, locked_until
	FROM uppgift.jobs
	WHERE id = $1
`, exec.record.ID).Scan(&state, &lastError, &lockedBy, &lockedUnt)
	require.NoError(t, err)

	assert.Equal(t, "discarded", state)
	require.NotNil(t, lastError)
	assert.Equal(t, testErr.Error(), *lastError)
	assert.Nil(t, lockedBy)
	assert.Nil(t, lockedUnt)

	// Verify finish() signaled
	select {
	case <-st.execDoneCh:
	default:
		t.Fatal("expected message on execDoneCh")
	}

	// Discarding again when not running should return ErrLeaseLost
	err = exec.Discard(ctx, testErr)
	require.ErrorIs(t, err, driver.ErrLeaseLost)
}

func TestExecution_WithTxContext(t *testing.T) {
	d, pool := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	workerID := "worker-test-tx"
	exec, _ := setupRunningExecution(t, d, pool, workerID)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	txCtx := WithTx(ctx, tx)
	err = exec.Complete(txCtx)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Verify job was deleted in committed transaction
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM uppgift.jobs WHERE id = $1;", exec.record.ID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestPurgeDiscarded(t *testing.T) {
	d, pool := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	now := time.Now().UTC()
	// Insert one old discarded job and one recent discarded job
	_, err := pool.Exec(ctx, `
		INSERT INTO uppgift.jobs (id, queue, kind, payload, state, attempt, max_retries, run_at, timeout_ms, enqueued_at)
		VALUES
			('old-discarded', 'default', 'task', '{}', 'discarded', 1, 3, $1, 300000, $2),
			('recent-discarded', 'default', 'task', '{}', 'discarded', 1, 3, $1, 300000, $3),
			('active-job', 'default', 'task', '{}', 'available', 0, 3, $1, 300000, $2);
	`, now, now.Add(-48*time.Hour), now.Add(-1*time.Hour))
	require.NoError(t, err)

	// Purge jobs older than 24 hours
	deleted, err := d.PurgeDiscarded(ctx, 24*time.Hour)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM uppgift.jobs WHERE id = 'old-discarded';").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM uppgift.jobs WHERE id = 'recent-discarded';").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM uppgift.jobs WHERE id = 'active-job';").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestStream_Validation(t *testing.T) {
	d, _ := setupTestDriver(t)
	ctx := context.Background()

	// Missing worker ID
	_, err := d.Stream(ctx, driver.StreamConfig{
		BufferSize: 4,
	})
	require.ErrorContains(t, err, "worker ID is required")

	// Invalid buffer size
	_, err = d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-1",
		BufferSize: 0,
	})
	require.ErrorContains(t, err, "buffer size must be greater than zero")

	_, err = d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-1",
		BufferSize: -1,
	})
	require.ErrorContains(t, err, "buffer size must be greater than zero")
}

func TestStream_BasicAndQueueFiltering(t *testing.T) {
	d, _ := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stream monitoring only the 'emails' queue
	stream, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "stream-worker-1",
		BufferSize: 4,
		Queues:     []string{"emails"},
	})
	require.NoError(t, err)
	defer stream.Close()

	// Enqueue a job in 'other' queue (should NOT be received)
	err = d.Enqueue(ctx, &driver.Record{
		ID:         uuid.New().String(),
		Kind:       "ignored_task",
		Queue:      "other",
		RunAt:      time.Now().UTC(),
		EnqueuedAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	// Enqueue a job in 'emails' queue (should be received)
	emailJobID := uuid.New().String()
	err = d.Enqueue(ctx, &driver.Record{
		ID:         emailJobID,
		Kind:       "send_welcome_email",
		Queue:      "emails",
		Payload:    []byte(`{"to":"test@example.com"}`),
		Attributes: map[string]string{"source": "signup"},
		RunAt:      time.Now().UTC(),
		EnqueuedAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	select {
	case exec, ok := <-stream.Next():
		require.True(t, ok)
		assert.Equal(t, emailJobID, exec.Record().ID)
		assert.Equal(t, "emails", exec.Record().Queue)
		assert.Equal(t, "send_welcome_email", exec.Record().Kind)
		assert.Equal(t, map[string]string{"source": "signup"}, exec.Record().Attributes)

		err := exec.Complete(ctx)
		require.NoError(t, err)

	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for execution on stream")
	}
}

func TestStream_DynamicTimerWakeup(t *testing.T) {
	d, _ := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	futureJobID := uuid.New().String()
	runAt := time.Now().UTC().Add(800 * time.Millisecond)

	// Enqueue future job before or after starting stream
	err := d.Enqueue(ctx, &driver.Record{
		ID:         futureJobID,
		Kind:       "future_task",
		Queue:      "default",
		RunAt:      runAt,
		EnqueuedAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	stream, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "timer-worker",
		BufferSize: 2,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer stream.Close()

	start := time.Now()
	select {
	case exec, ok := <-stream.Next():
		require.True(t, ok)
		assert.Equal(t, futureJobID, exec.Record().ID)
		elapsed := time.Since(start)
		assert.GreaterOrEqual(t, elapsed, 700*time.Millisecond, "expected execution to wait until scheduled run_at")
		require.NoError(t, exec.Complete(ctx))

	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for future execution")
	}
}

func TestStream_ZombieLeaseReclamation(t *testing.T) {
	d, pool := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	zombieJobID := uuid.New().String()
	now := time.Now().UTC()

	// Insert a job locked by a crashed worker whose lease has expired
	_, err := pool.Exec(ctx, `
		INSERT INTO uppgift.jobs (id, queue, kind, payload, state, attempt, max_retries, run_at, timeout_ms, locked_by, locked_until, enqueued_at)
		VALUES ($1, 'default', 'zombie_task', '{}', 'running', 1, 3, $2, 300000, 'crashed-worker', $3, $2);
	`, zombieJobID, now.Add(-10*time.Minute), now.Add(-1*time.Minute))
	require.NoError(t, err)

	stream, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "reclaimer-worker",
		BufferSize: 2,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer stream.Close()

	select {
	case exec, ok := <-stream.Next():
		require.True(t, ok)
		assert.Equal(t, zombieJobID, exec.Record().ID)
		assert.Equal(t, 1, exec.Record().Attempt) // Attempt count preserved for worker to handle
		require.NoError(t, exec.Complete(ctx))

	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for zombie job to be reclaimed")
	}
}

func TestStream_GracefulShutdownBufferRelease(t *testing.T) {
	d, pool := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Enqueue 3 available jobs
	for i := 0; i < 3; i++ {
		err := d.Enqueue(ctx, &driver.Record{
			ID:         uuid.New().String(),
			Kind:       "buffer_release_task",
			Queue:      "default",
			RunAt:      time.Now().UTC(),
			EnqueuedAt: time.Now().UTC(),
		})
		require.NoError(t, err)
	}

	// Start stream with buffer size 5 so it claims all 3 jobs into memory
	stream, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "shutting-down-worker",
		BufferSize: 5,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)

	// Consume 1 job from stream and leave 2 in memory buffer
	select {
	case exec, ok := <-stream.Next():
		require.True(t, ok)
		require.NoError(t, exec.Complete(ctx))
	case <-time.After(3 * time.Second):
		t.Fatal("timed out consuming first job")
	}

	// Close stream gracefully
	err = stream.Close()
	require.NoError(t, err)

	// Verify the remaining 2 jobs were released back to 'available' with locked_by=NULL
	var (
		availableCount int
		runningCount   int
	)
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM uppgift.jobs WHERE state = 'available' AND locked_by IS NULL;").Scan(&availableCount)
	require.NoError(t, err)
	assert.Equal(t, 2, availableCount)

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM uppgift.jobs WHERE state = 'running';").Scan(&runningCount)
	require.NoError(t, err)
	assert.Equal(t, 0, runningCount)
}

type testTaskArgs struct {
	Msg string `json:"msg"`
}

func (testTaskArgs) Kind() string {
	return "test_task"
}

func TestWorker_FullIntegration(t *testing.T) {
	d, _ := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mux := uppgift.NewMux()

	var (
		processedJobs sync.Map
		wg            sync.WaitGroup
	)

	wg.Add(3)

	mux.Register[testTaskArgs](uppgift.HandlerFunc[testTaskArgs](func(ctx context.Context, job *uppgift.Job[testTaskArgs]) error {
		processedJobs.Store(job.ID, job.Args.Msg)
		wg.Done()
		return nil
	}))

	worker := uppgift.NewWorker(d, mux,
		uppgift.WithConcurrency(4),
		uppgift.WithQueues("default"),
	)

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	workerDone := make(chan error, 1)
	go func() {
		workerDone <- worker.Start(workerCtx)
	}()

	// Enqueue 3 jobs
	ids := []string{uuid.New().String(), uuid.New().String(), uuid.New().String()}
	for i, id := range ids {
		err := d.Enqueue(ctx, &driver.Record{
			ID:         id,
			Kind:       "test_task",
			Queue:      "default",
			Payload:    fmt.Appendf(nil, `{"msg":"task-%d"}`, i),
			RunAt:      time.Now().UTC(),
			EnqueuedAt: time.Now().UTC(),
		})
		require.NoError(t, err)
	}

	// Wait for all 3 jobs to complete
	wg.Wait()

	for i, id := range ids {
		val, ok := processedJobs.Load(id)
		assert.True(t, ok)
		assert.Equal(t, fmt.Sprintf("task-%d", i), val)
	}

	// Stop worker gracefully
	workerCancel()
	err := <-workerDone
	require.ErrorIs(t, err, context.Canceled)
}

func TestPing(t *testing.T) {
	d, _ := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	require.NoError(t, d.Ping(ctx))
}

func TestStream_CompetingWorkersContention(t *testing.T) {
	d, pool := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const jobCount = 30
	const workerCount = 3

	// Enqueue 30 jobs
	for i := 0; i < jobCount; i++ {
		err := d.Enqueue(ctx, &driver.Record{
			ID:         uuid.New().String(),
			Kind:       "contention_task",
			Queue:      "default",
			RunAt:      time.Now().UTC(),
			EnqueuedAt: time.Now().UTC(),
		})
		require.NoError(t, err)
	}

	var (
		processedJobs sync.Map
		processedMu   sync.Mutex
		totalCount    int
		wg            sync.WaitGroup
	)

	streams := make([]driver.Stream, workerCount)
	for w := 0; w < workerCount; w++ {
		workerID := fmt.Sprintf("worker-%d", w)
		stream, err := d.Stream(ctx, driver.StreamConfig{
			WorkerID:   workerID,
			BufferSize: 4,
			Queues:     []string{"default"},
		})
		require.NoError(t, err)
		streams[w] = stream
		defer stream.Close()

		wg.Add(1)
		go func(s driver.Stream, id string) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case exec, ok := <-s.Next():
					if !ok {
						return
					}
					jobID := exec.Record().ID
					// Ensure no other worker has claimed this exact job
					if _, loaded := processedJobs.LoadOrStore(jobID, id); loaded {
						t.Errorf("duplicate claim detected on job %s by worker %s", jobID, id)
					}

					require.NoError(t, exec.Complete(ctx))

					processedMu.Lock()
					totalCount++
					done := totalCount == jobCount
					processedMu.Unlock()

					if done {
						return
					}
				}
			}
		}(stream, workerID)
	}

	// Wait for all jobs to be processed
	require.Eventually(t, func() bool {
		processedMu.Lock()
		defer processedMu.Unlock()
		return totalCount == jobCount
	}, 5*time.Second, 50*time.Millisecond)

	// Verify all 30 jobs were deleted from database
	var dbCount int
	err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM uppgift.jobs;").Scan(&dbCount)
	require.NoError(t, err)
	assert.Equal(t, 0, dbCount)
}

func TestStream_BacklogDraining(t *testing.T) {
	d, pool := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const jobCount = 25

	// Enqueue 25 jobs before starting worker
	for i := 0; i < jobCount; i++ {
		err := d.Enqueue(ctx, &driver.Record{
			ID:         uuid.New().String(),
			Kind:       "backlog_task",
			Queue:      "default",
			RunAt:      time.Now().UTC(),
			EnqueuedAt: time.Now().UTC(),
		})
		require.NoError(t, err)
	}

	// Start stream with small BufferSize=4 to force multiple batch claims
	stream, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "backlog-drainer",
		BufferSize: 4,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer stream.Close()

	received := 0
	for received < jobCount {
		select {
		case exec, ok := <-stream.Next():
			require.True(t, ok)
			require.NoError(t, exec.Complete(ctx))
			received++
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out draining backlog: processed %d/%d", received, jobCount)
		}
	}

	assert.Equal(t, jobCount, received)

	var dbCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM uppgift.jobs;").Scan(&dbCount)
	require.NoError(t, err)
	assert.Equal(t, 0, dbCount)
}

func TestStream_LateExecutionLeaseLost(t *testing.T) {
	d, pool := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	jobID := uuid.New().String()
	now := time.Now().UTC()

	// Enqueue job with short timeout
	err := d.Enqueue(ctx, &driver.Record{
		ID:         jobID,
		Kind:       "slow_task",
		Queue:      "default",
		Timeout:    100 * time.Millisecond,
		RunAt:      now,
		EnqueuedAt: now,
	})
	require.NoError(t, err)

	// Worker 1 claims job
	stream1, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "slow-worker-1",
		BufferSize: 1,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer stream1.Close()

	var exec1 driver.Execution
	select {
	case exec1 = <-stream1.Next():
		assert.Equal(t, jobID, exec1.Record().ID)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream1 execution")
	}

	// Simulate slow execution: manually expire lease in DB
	_, err = pool.Exec(ctx, `
		UPDATE uppgift.jobs
		SET locked_until = now() - interval '1 second'
		WHERE id = $1;
	`, jobID)
	require.NoError(t, err)

	// Worker 2 starts and reclaims the expired job
	stream2, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "reclaimer-worker-2",
		BufferSize: 1,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer stream2.Close()

	var exec2 driver.Execution
	select {
	case exec2 = <-stream2.Next():
		assert.Equal(t, jobID, exec2.Record().ID)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream2 reclamation")
	}

	// Worker 1 finishes late and tries to complete -> lease lost!
	err = exec1.Complete(ctx)
	require.ErrorIs(t, err, driver.ErrLeaseLost)

	// Worker 2 completes successfully
	err = exec2.Complete(ctx)
	require.NoError(t, err)

	// Job is deleted
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM uppgift.jobs WHERE id = $1;", jobID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestStream_ListenerReconnection(t *testing.T) {
	d, pool := setupTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "reconnect-worker",
		BufferSize: 2,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer stream.Close()

	// Terminate the listener backend connection from Postgres
	_, err = pool.Exec(ctx, `
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE query LIKE 'LISTEN uppgift_events%'
		  AND pid <> pg_backend_pid();
	`)
	require.NoError(t, err)

	// Give the listener time to detect disconnect and reconnect with backoff
	time.Sleep(300 * time.Millisecond)

	// Enqueue new job after connection was severed
	jobID := uuid.New().String()
	err = d.Enqueue(ctx, &driver.Record{
		ID:         jobID,
		Kind:       "post_reconnect_task",
		Queue:      "default",
		RunAt:      time.Now().UTC(),
		EnqueuedAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	// Verify stream reconnected and claimed the job via pg_notify
	select {
	case exec, ok := <-stream.Next():
		require.True(t, ok)
		assert.Equal(t, jobID, exec.Record().ID)
		require.NoError(t, exec.Complete(ctx))
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for execution after listener reconnect")
	}
}

func TestStream_ContextCancel_Explicit(t *testing.T) {
	d, _ := setupTestDriver(t)
	streamCtx, cancel := context.WithCancel(context.Background())

	stream, err := d.Stream(streamCtx, driver.StreamConfig{
		WorkerID:   "cancel-worker",
		BufferSize: 2,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)

	// Explicitly cancel the context
	cancel()

	// Next() channel must close
	select {
	case _, ok := <-stream.Next():
		require.False(t, ok, "expected Next channel to be closed on context cancellation")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream.Next() to close")
	}

	// stream.Err() must return context.Canceled
	require.ErrorIs(t, stream.Err(), context.Canceled)

	// Stream.Close() should complete cleanly
	require.NoError(t, stream.Close())
}

func TestStream_ContextCancel_Timeout(t *testing.T) {
	d, _ := setupTestDriver(t)
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	stream, err := d.Stream(timeoutCtx, driver.StreamConfig{
		WorkerID:   "timeout-worker",
		BufferSize: 2,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer stream.Close()

	// Wait for deadline to expire and channel to close
	select {
	case _, ok := <-stream.Next():
		require.False(t, ok, "expected Next channel to be closed on deadline expiration")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream.Next() to close on deadline")
	}

	// stream.Err() must return context.DeadlineExceeded
	require.ErrorIs(t, stream.Err(), context.DeadlineExceeded)
}

func TestStream_ContextCancel_BufferCleanup(t *testing.T) {
	d, pool := setupTestDriver(t)
	ctx := context.Background()

	// Enqueue 3 available jobs
	for i := 0; i < 3; i++ {
		err := d.Enqueue(ctx, &driver.Record{
			ID:         uuid.New().String(),
			Kind:       "cancel_cleanup_task",
			Queue:      "default",
			RunAt:      time.Now().UTC(),
			EnqueuedAt: time.Now().UTC(),
		})
		require.NoError(t, err)
	}

	streamCtx, cancel := context.WithCancel(ctx)

	// Stream with buffer size 5 claims all 3 jobs into memory buffer
	stream, err := d.Stream(streamCtx, driver.StreamConfig{
		WorkerID:   "cancel-cleanup-worker",
		BufferSize: 5,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)

	// Consume 1 job, leaving 2 in memory buffer
	select {
	case exec, ok := <-stream.Next():
		require.True(t, ok)
		require.NoError(t, exec.Complete(ctx))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out consuming first job")
	}

	// Cancel the stream context while 2 jobs are held in stream buffer
	cancel()

	// Wait for stream to terminate via Next() channel closure
	select {
	case _, ok := <-stream.Next():
		require.False(t, ok, "expected Next channel to be closed")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream to terminate on cancellation")
	}

	require.ErrorIs(t, stream.Err(), context.Canceled)

	// Verify the 2 remaining buffered jobs were released back to 'available' in DB via detached context
	var (
		availableCount int
		runningCount   int
	)
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM uppgift.jobs WHERE state = 'available' AND locked_by IS NULL;").Scan(&availableCount)
	require.NoError(t, err)
	assert.Equal(t, 2, availableCount)

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM uppgift.jobs WHERE state = 'running';").Scan(&runningCount)
	require.NoError(t, err)
	assert.Equal(t, 0, runningCount)
}

func TestExecution_CompleteAfterStreamContextCanceled(t *testing.T) {
	d, pool := setupTestDriver(t)
	streamCtx, cancel := context.WithCancel(context.Background())

	jobID := uuid.New().String()
	err := d.Enqueue(context.Background(), &driver.Record{
		ID:         jobID,
		Kind:       "in_flight_cancel_task",
		Queue:      "default",
		RunAt:      time.Now().UTC(),
		EnqueuedAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	stream, err := d.Stream(streamCtx, driver.StreamConfig{
		WorkerID:   "in-flight-worker",
		BufferSize: 2,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)

	// Receive the job into worker goroutine
	var exec driver.Execution
	select {
	case exec = <-stream.Next():
		assert.Equal(t, jobID, exec.Record().ID)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out receiving execution")
	}

	// Stream context is canceled (e.g. shutdown initiated)
	cancel()

	// Wait for stream to finish closing
	for range stream.Next() {
	}

	// The in-flight job finishes its work and calls Complete using a detached ack context (as Worker does)
	ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(streamCtx), 2*time.Second)
	defer ackCancel()

	err = exec.Complete(ackCtx)
	require.NoError(t, err)

	// Verify job was deleted
	var count int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM uppgift.jobs WHERE id = $1;", jobID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}
