// Package pgx provides a PostgreSQL storage driver for uppgift backed by jackc/pgx.
//
// # Overview
//
// The pgx package implements the [driver.Driver] interface using PostgreSQL for durable
// background job persistence, reactive streaming, and distributed lease coordination.
//
// Key features include:
//
//   - Zero-polling event loop: Uses PostgreSQL LISTEN and NOTIFY ("uppgift_events") combined with dynamic wake-up timers to dispatch jobs with minimal latency and zero database queries while idle.
//   - Concurrency and contention control: Employs atomic CTE queries with SELECT ... FOR UPDATE SKIP LOCKED to claim job batches without worker contention or lock starvation.
//   - Transactional enqueuing (Outbox pattern): Supports [WithTx] and [TxFromContext] to atomically insert jobs in the same database transaction as application business data.
//   - Self-healing leases: Tracks job execution leases with expiration deadlines (locked_until), automatically reclaiming zombie jobs if a worker crashes without requiring background reaper tasks or heartbeats.
//   - Graceful buffer release: Releases un-dispatched in-memory buffer jobs back to PostgreSQL during worker shutdown, waking peer workers immediately.
//   - Embedded schema migrations: Includes embedded SQL migrations managed with Goose via [Driver.Migrate].
//
// # Initialize the driver
//
// Create a [Driver] by providing a [*pgxpool.Pool]:
//
//	pool, err := pgxpool.New(ctx, "postgres://user:pass@localhost:5432/dbname")
//	if err != nil {
//		log.Fatalf("unable to connect to database: %v", err)
//	}
//	defer pool.Close()
//
//	d := pgx.NewDriver(pool)
//
// Use [Driver.Ping] to verify connectivity to the PostgreSQL cluster.
//
// # Database migrations
//
// Run [Driver.Migrate] to apply schema migrations.
// Migrations create the "uppgift" schema, the "uppgift.jobs" table, necessary indexes,
// and the "uppgift.schema_migrations" tracking table:
//
//	if err := d.Migrate(ctx); err != nil {
//		log.Fatalf("failed to run migrations: %v", err)
//	}
//
// Migrations are embedded in the driver binary and are safe to run concurrently across multiple instances.
//
// # Enqueue jobs
//
// Pass the [Driver] to [uppgift.NewEnqueuer] to enqueue jobs:
//
//	enq := uppgift.NewEnqueuer(d)
//
//	id, err := enq.Enqueue(ctx, &EmailJob{
//		To:      "user@example.com",
//		Subject: "Welcome",
//	})
//
// [Driver.Enqueue] and [Driver.EnqueueBatch] insert jobs in the "available" state and notify the
// corresponding queues via pg_notify.
//
// When enqueuing jobs with a unique key (configured via [uppgift.WithUniqueKey]), the driver utilizes a
// PostgreSQL partial unique index to ignore duplicates while an active job with that key is pending or running.
//
// # Transactional enqueuing (Outbox pattern)
//
// To guarantee that a job is only enqueued if business data is successfully committed to PostgreSQL,
// attach an active [pgx.Tx] to the context with [WithTx]:
//
//	tx, err := pool.Begin(ctx)
//	if err != nil {
//		return err
//	}
//	defer tx.Rollback(ctx)
//
//	// Perform business logic within the transaction
//	if _, err := tx.Exec(ctx, "INSERT INTO users (name, email) VALUES ($1, $2)", "Alice", "alice@example.com"); err != nil {
//		return err
//	}
//
//	// Enqueue the job within the same transaction
//	txCtx := pgx.WithTx(ctx, tx)
//	if _, err := enq.Enqueue(txCtx, &SendWelcomeEmailJob{Email: "alice@example.com"}); err != nil {
//		return err
//	}
//
//	// Commit the transaction; the job and business data are committed atomically
//	return tx.Commit(ctx)
//
// If the transaction rolls back, no job is persisted and no notifications are emitted.
//
// # Process jobs with Worker
//
// Pass the [Driver] and a [uppgift.Mux] to [uppgift.NewWorker]:
//
//	w := uppgift.NewWorker(d, mux,
//		uppgift.WithConcurrency(16),
//		uppgift.WithQueues("emails", "default"),
//		uppgift.WithShutdownTimeout(30 * time.Second),
//	)
//
//	if err := w.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
//		log.Fatalf("worker error: %v", err)
//	}
//
// # Job streaming and execution leases
//
// The worker receives jobs through [Driver.Stream]:
//
//   - Dedicated listener: The stream reserves a connection from the pool to run LISTEN uppgift_events, awakening workers when jobs are enqueued, retried, or snoozed.
//   - Dynamic wake timers: If no jobs are currently claimable but upcoming scheduled jobs or expiring leases exist, the stream arms an internal timer for the earliest next_run_at timestamp.
//   - Capacity-aware claiming: The stream claims batches with FOR UPDATE SKIP LOCKED only when free execution slots are available, ensuring memory buffers never hold more jobs than the worker's buffer size.
//   - Lease fencing: Claimed jobs are assigned an exclusive lease with locked_by set to the worker ID and locked_until set to the execution timeout plus a 30-second grace period.
//   - Zombie lease reclamation: If a worker terminates abruptly without completing its jobs, other workers automatically reclaim the jobs once locked_until expires.
//   - Graceful shutdown: When the stream stops, un-dispatched jobs held in the local memory buffer are immediately returned to the "available" state in PostgreSQL and peer workers are notified.
//
// # Discarded jobs and maintenance
//
// When a job exceeds its maximum retry attempts or the handler returns [uppgift.DiscardJob],
// the driver marks the job state as "discarded" and records the error message in the last_error column.
//
// Discarded jobs remain in the database for auditing and inspection. Use [Driver.PurgeDiscarded]
// to periodically delete old discarded records:
//
//	// Delete dead-letter jobs discarded more than 30 days ago
//	deleted, err := d.PurgeDiscarded(ctx, 30*24*time.Hour)
//	if err != nil {
//		log.Printf("failed to purge discarded jobs: %v", err)
//	}
//
// For core uppgift types and worker configuration, see the [github.com/branchgrove/pogo/uppgift] package.
// For storage driver definitions, see the [github.com/branchgrove/pogo/uppgift/driver] package.
package pgx
