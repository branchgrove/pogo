package uppgift

import "time"

// Meta contains runtime and transport metadata for an enqueued job.
type Meta struct {
	// ID is the unique identifier of the job instance.
	ID string
	// Attributes contains custom string key-value metadata.
	Attributes map[string]string
	// Attempt is the current execution attempt number, starting at 0.
	Attempt int
	// EnqueuedAt is the timestamp when the job was enqueued.
	EnqueuedAt time.Time
	// Queue is the name of the queue that contains the job.
	Queue string
	// MaxRetries is the maximum number of retry attempts allowed after failure.
	MaxRetries int
	// UniqueKey is the deduplication key for the job.
	UniqueKey string
	// Timeout is the maximum execution duration for an attempt.
	Timeout time.Duration
	// RunAt is the timestamp when the job is eligible for execution.
	RunAt time.Time
}

// Job contains runtime [Meta] and the typed payload [JobArgs] for a job execution.
type Job[T JobArgs] struct {
	Meta
	Args T
}

// JobArgs defines the kind and payload schema of a job.
type JobArgs interface {
	// Kind returns the unique string identifier for this job type.
	// The method must use a value receiver.
	Kind() string
}

// JobDefaults defines default [JobOption] values for a [JobArgs] type when enqueued.
type JobDefaults interface {
	JobArgs
	JobOptions() []JobOption
}

type jobConfig struct {
	queue      string
	runAt      time.Time
	maxRetries int
	timeout    time.Duration
	uniqueKey  string
	attributes map[string]string
}

// WithQueue sets the destination queue for the job.
// The default queue applied by [NewEnqueuer] is "default".
func WithQueue(q string) JobOption {
	return func(c *jobConfig) { c.queue = q }
}

// WithDelay schedules the job to run after duration d from the current time.
// By default, a job runs immediately.
func WithDelay(d time.Duration) JobOption {
	return func(c *jobConfig) { c.runAt = time.Now().UTC().Add(d) }
}

// WithRunAt schedules the job to run at the specified time.
// By default, a job runs immediately at the time it is enqueued.
func WithRunAt(t time.Time) JobOption {
	return func(c *jobConfig) { c.runAt = t.UTC() }
}

// WithMaxRetries sets the maximum number of retry attempts for the job.
// A negative value enables unlimited retries. A value of 0 disables retries.
// The default value applied by [NewEnqueuer] is 25.
func WithMaxRetries(n int) JobOption {
	return func(c *jobConfig) { c.maxRetries = n }
}

// WithTimeout sets the maximum execution duration for the job.
// Handlers must monitor [context.Context.Done] to obey this limit.
// A value of 0 disables the per-job execution timeout.
func WithTimeout(d time.Duration) JobOption {
	return func(c *jobConfig) { c.timeout = d }
}

// WithUniqueKey sets a deduplication key. Only one job with this key can
// exist concurrently in pending ('available') or running ('running') state.
// Once a job completes or is discarded, a new job with the same unique key
// can be enqueued.
// By default, deduplication is not enabled.
func WithUniqueKey(key string) JobOption {
	return func(c *jobConfig) { c.uniqueKey = key }
}

// WithAttribute sets a metadata attribute key-value pair. If the key exists,
// WithAttribute replaces its value.
func WithAttribute(key string, value string) JobOption {
	return func(c *jobConfig) {
		if c.attributes == nil {
			c.attributes = make(map[string]string, 1)
		}
		c.attributes[key] = value
	}
}

// WithAttributes sets metadata attributes. WithAttributes replaces all existing attributes.
func WithAttributes(attributes map[string]string) JobOption {
	return func(c *jobConfig) {
		c.attributes = attributes
	}
}

// JobOption configures an individual job.
type JobOption func(*jobConfig)
