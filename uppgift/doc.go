// Package uppgift provides background job processing with type-safe job definitions,
// routing, enqueuing, and concurrent execution.
//
// # Overview
//
// The uppgift package divides background job processing into four primary components:
//
//   - [JobArgs]: Defines the kind and typed payload of a job.
//   - [Enqueuer]: Serializes and writes jobs to a [driver.Driver].
//   - [Mux]: Routes jobs by kind to registered handlers.
//   - [Worker]: Reads jobs from a [driver.Driver] and executes them concurrently.
//
// # Define jobs
//
// A job payload is a struct that implements [JobArgs].
// The [JobArgs.Kind] method returns a unique string identifier for the job type.
// The [JobArgs.Kind] method must use a value receiver.
//
//	type EmailJob struct {
//		To      string `json:"to"`
//		Subject string `json:"subject"`
//		Body    string `json:"body"`
//	}
//
//	func (EmailJob) Kind() string {
//		return "email:send"
//	}
//
// To configure default options for a job type, implement [JobDefaults]:
//
//	func (EmailJob) JobOptions() []uppgift.JobOption {
//		return []uppgift.JobOption{
//			uppgift.WithQueue("emails"),
//			uppgift.WithMaxRetries(5),
//			uppgift.WithTimeout(30 * time.Second),
//		}
//	}
//
// # Enqueue jobs
//
// Use [NewEnqueuer] to create an enqueuer with a storage driver:
//
//	enq := uppgift.NewEnqueuer(d)
//
// [NewEnqueuer] applies default options to all enqueued jobs:
//
//   - Queue: "default" (configured with [WithQueue])
//   - MaxRetries: 25 (configured with [WithMaxRetries])
//   - Timeout: 5 minutes (configured with [WithTimeout])
//
// Call [Enqueuer.Enqueue] to insert a job into the queue:
//
//	id, err := enq.Enqueue(ctx, &EmailJob{
//		To:      "user@example.com",
//		Subject: "Welcome",
//		Body:    "Hello!",
//	})
//
// Pass [JobOption] values to override defaults or set additional options:
//
//	id, err := enq.Enqueue(ctx, &EmailJob{To: "user@example.com"},
//		uppgift.WithDelay(5 * time.Minute),
//		uppgift.WithUniqueKey("user:123:welcome"),
//	)
//
// # Deduplication with Unique Keys
//
// Use [WithUniqueKey] to prevent duplicate jobs from being enqueued while an identical job is active:
//
//   - When a job is enqueued with a unique key, subsequent enqueue attempts with the same key are ignored while the original job is pending execution or actively running.
//   - Once the active job finishes (either marked complete or discarded), a new job with that unique key can be enqueued.
//
// # Option precedence
//
// When you enqueue a job, the package resolves options in the following order (lowest to highest precedence):
//
//  1. Default options configured on the [Enqueuer] (Queue: "default", MaxRetries: 25, Timeout: 5 minutes).
//  2. Default options returned by [JobDefaults.JobOptions].
//  3. Options passed directly to [Enqueuer.Enqueue].
//
// # Handle jobs
//
// A job handler processes a specific job type.
// Implement the [Handler] interface or use the [HandlerFunc] adapter:
//
//	type EmailHandler struct {
//		client *email.Client
//	}
//
//	func (h *EmailHandler) Handle(ctx context.Context, job *uppgift.Job[EmailJob]) error {
//		return h.client.Send(ctx, job.Args.To, job.Args.Subject, job.Args.Body)
//	}
//
// Each job execution receives a [Job] instance that contains:
//
//   - Args: The typed payload unmarshaled from JSON.
//   - Meta: A [Meta] struct that contains execution metadata such as [Meta.Attempt], [Meta.EnqueuedAt], and [Meta.Queue].
//
// # Route jobs with Mux
//
// Use [NewMux] to create a multiplexer.
// Register handlers for job kinds during application initialization:
//
//	mux := uppgift.NewMux()
//	mux.Register(emailHandler)
//
// The type parameter T in [Mux.Register] must be a struct value type.
// [Mux.Register] panics if you pass a pointer type or if a handler already exists for the kind.
//
// # Execution control and retries
//
// Handlers control job execution with their return value:
//
//   - Success: Return nil. The worker marks the execution as complete and deletes the job from storage.
//   - Failure: Return an error. The worker reschedules the job with exponential backoff.
//   - Discard: Return [DiscardJob]. The worker unschedules the job without further retries.
//   - Snooze: Return [SnoozeJob] or [SnoozeJobFor]. The worker postpones the job without incrementing the attempt count.
//
// For example, discard a job when an unrecoverable validation error occurs:
//
//	if err := job.Args.Validate(); err != nil {
//		return uppgift.DiscardJob(err)
//	}
//
// Snooze a job when an external service is rate-limited:
//
//	if rateLimited {
//		return uppgift.SnoozeJobFor(10 * time.Minute)
//	}
//
// # Process jobs with Worker
//
// Use [NewWorker] to create a worker that reads from a driver and dispatches jobs to a mux:
//
//	w := uppgift.NewWorker(d, mux,
//		uppgift.WithConcurrency(16),
//		uppgift.WithQueues("emails", "default"),
//		uppgift.WithShutdownTimeout(30 * time.Second),
//	)
//
// [NewWorker] applies the following default configuration:
//
//   - Concurrency: 32 (configured with [WithConcurrency])
//   - Queues: []string{"default"} (configured with [WithQueues])
//   - ID: A random UUID string (configured with [WithID])
//   - ShutdownTimeout: 25 seconds (configured with [WithShutdownTimeout])
//   - BackoffFunc: Exponential backoff with equal jitter from 1 second up to 24 hours (configured with [WithBackoffFunc])
//
// Call [Worker.Start] to start processing jobs:
//
//	if err := w.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
//		log.Fatalf("worker error: %v", err)
//	}
//
// Don't call [Worker.Start] multiple times on the same worker instance.
// Configure concurrency with [WithConcurrency] instead.
//
// When the context is canceled, the worker stops accepting new jobs.
// The worker waits for active jobs to complete until the shutdown timeout expires.
//
// # Storage drivers
//
// Storage backends implement the [driver.Driver] interface.
// Drivers manage job persistence, streaming, and execution leases.
// For driver definitions, see the [github.com/branchgrove/pogo/uppgift/driver] package.
package uppgift
