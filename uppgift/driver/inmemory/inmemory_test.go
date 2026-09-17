package inmemory_test

import (
	"context"
	"testing"
	"time"

	"github.com/branchgrove/pogo/uppgift"
	"github.com/branchgrove/pogo/uppgift/driver"
	"github.com/branchgrove/pogo/uppgift/driver/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnqueueAndStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d := inmemory.New()

	rec := &driver.Record{
		ID:         "job-1",
		Kind:       "test-kind",
		Queue:      "default",
		Payload:    []byte(`{"msg":"hello"}`),
		Attributes: map[string]string{"key": "value"},
		RunAt:      time.Now().UTC(),
		EnqueuedAt: time.Now().UTC(),
	}

	err := d.Enqueue(ctx, rec)
	require.NoError(t, err)

	s, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-1",
		BufferSize: 1,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer s.Close()

	select {
	case exec, ok := <-s.Next():
		require.True(t, ok)
		assert.Equal(t, "job-1", exec.Record().ID)
		assert.Equal(t, "test-kind", exec.Record().Kind)
		assert.Equal(t, []byte(`{"msg":"hello"}`), exec.Record().Payload)
		assert.Equal(t, "value", exec.Record().Attributes["key"])

		err = exec.Complete(ctx)
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for job")
	}
}

func TestUniqueKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d := inmemory.New()

	rec1 := &driver.Record{
		ID:        "job-1",
		Kind:      "test-kind",
		Queue:     "default",
		UniqueKey: "uniq-123",
		RunAt:     time.Now().UTC(),
	}
	rec2 := &driver.Record{
		ID:        "job-2",
		Kind:      "test-kind",
		Queue:     "default",
		UniqueKey: "uniq-123",
		RunAt:     time.Now().UTC(),
	}

	err := d.Enqueue(ctx, rec1)
	require.NoError(t, err)

	err = d.Enqueue(ctx, rec2)
	require.NoError(t, err)

	s, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-1",
		BufferSize: 2,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer s.Close()

	var exec driver.Execution
	select {
	case exec = <-s.Next():
		assert.Equal(t, "job-1", exec.Record().ID)
	case <-ctx.Done():
		t.Fatal("timeout waiting for job")
	}

	// Ensure second duplicate job was not queued.
	select {
	case <-s.Next():
		t.Fatal("unexpected second job received")
	case <-time.After(100 * time.Millisecond):
	}

	err = exec.Complete(ctx)
	require.NoError(t, err)

	// After complete, same unique key can be enqueued again.
	err = d.Enqueue(ctx, rec2)
	require.NoError(t, err)

	select {
	case exec2 := <-s.Next():
		assert.Equal(t, "job-2", exec2.Record().ID)
		err = exec2.Complete(ctx)
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for re-enqueued job")
	}
}

func TestFailAndRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d := inmemory.New()

	rec := &driver.Record{
		ID:    "job-retry",
		Kind:  "test-kind",
		Queue: "default",
		RunAt: time.Now().UTC(),
	}

	err := d.Enqueue(ctx, rec)
	require.NoError(t, err)

	s, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-1",
		BufferSize: 1,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer s.Close()

	// First attempt
	exec := <-s.Next()
	require.NotNil(t, exec)
	assert.Equal(t, 0, exec.Record().Attempt)

	// Fail and schedule immediately
	err = exec.Fail(ctx, time.Now().UTC(), 1, assert.AnError)
	require.NoError(t, err)

	// Second attempt
	exec2 := <-s.Next()
	require.NotNil(t, exec2)
	assert.Equal(t, 1, exec2.Record().Attempt)

	err = exec2.Complete(ctx)
	require.NoError(t, err)
}

func TestSnooze(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d := inmemory.New()

	rec := &driver.Record{
		ID:    "job-snooze",
		Kind:  "test-kind",
		Queue: "default",
		RunAt: time.Now().UTC(),
	}

	err := d.Enqueue(ctx, rec)
	require.NoError(t, err)

	s, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-1",
		BufferSize: 1,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer s.Close()

	exec := <-s.Next()
	require.NotNil(t, exec)

	err = exec.Snooze(ctx, time.Now().UTC().Add(50*time.Millisecond))
	require.NoError(t, err)

	select {
	case exec2 := <-s.Next():
		assert.Equal(t, "job-snooze", exec2.Record().ID)
		err = exec2.Complete(ctx)
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for snoozed job")
	}
}

func TestDiscard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d := inmemory.New()

	rec := &driver.Record{
		ID:        "job-discard",
		Kind:      "test-kind",
		Queue:     "default",
		UniqueKey: "uniq-discard",
		RunAt:     time.Now().UTC(),
	}

	err := d.Enqueue(ctx, rec)
	require.NoError(t, err)

	s, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-1",
		BufferSize: 1,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer s.Close()

	exec := <-s.Next()
	require.NotNil(t, exec)

	err = exec.Discard(ctx, assert.AnError)
	require.NoError(t, err)

	// Verify key was released
	err = d.Enqueue(ctx, rec)
	require.NoError(t, err)
}

func TestQueueFiltering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d := inmemory.New()

	err := d.Enqueue(ctx, &driver.Record{
		ID:    "job-queue-a",
		Kind:  "test-kind",
		Queue: "queue-a",
		RunAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	err = d.Enqueue(ctx, &driver.Record{
		ID:    "job-queue-b",
		Kind:  "test-kind",
		Queue: "queue-b",
		RunAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	s, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-1",
		BufferSize: 2,
		Queues:     []string{"queue-b"},
	})
	require.NoError(t, err)
	defer s.Close()

	select {
	case exec := <-s.Next():
		assert.Equal(t, "job-queue-b", exec.Record().ID)
		err = exec.Complete(ctx)
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for queue-b job")
	}

	select {
	case <-s.Next():
		t.Fatal("unexpected job received from different queue")
	case <-time.After(100 * time.Millisecond):
	}
}

type testJobArgs struct {
	Greeting string
}

func (testJobArgs) Kind() string {
	return "test_job"
}

func TestEnqueueEmptyID(t *testing.T) {
	ctx := context.Background()
	d := inmemory.New()

	err := d.Enqueue(ctx, &driver.Record{
		ID:    "",
		Kind:  "test-kind",
		Queue: "default",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty record ID")
}

func TestMultipleStreamsBroadcast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d := inmemory.New()

	sA, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-a",
		BufferSize: 1,
		Queues:     []string{"queue-a"},
	})
	require.NoError(t, err)
	defer sA.Close()

	sB, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-b",
		BufferSize: 1,
		Queues:     []string{"queue-b"},
	})
	require.NoError(t, err)
	defer sB.Close()

	// Enqueue to queue-b first.
	err = d.Enqueue(ctx, &driver.Record{
		ID:    "job-b1",
		Kind:  "kind-b",
		Queue: "queue-b",
		RunAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	select {
	case execB := <-sB.Next():
		assert.Equal(t, "job-b1", execB.Record().ID)
		err = execB.Complete(ctx)
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for stream B job")
	}

	// Enqueue to queue-a second.
	err = d.Enqueue(ctx, &driver.Record{
		ID:    "job-a1",
		Kind:  "kind-a",
		Queue: "queue-a",
		RunAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	select {
	case execA := <-sA.Next():
		assert.Equal(t, "job-a1", execA.Record().ID)
		err = execA.Complete(ctx)
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for stream A job")
	}
}

func TestStreamCloseRevertsBufferedJobs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d := inmemory.New()

	for i := 1; i <= 3; i++ {
		err := d.Enqueue(ctx, &driver.Record{
			ID:         "job-" + time.Duration(i).String(),
			Kind:       "kind",
			Queue:      "default",
			RunAt:      time.Now().UTC(),
			EnqueuedAt: time.Now().UTC().Add(time.Duration(i) * time.Millisecond),
		})
		require.NoError(t, err)
	}

	s1, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-1",
		BufferSize: 5,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)

	// Close stream 1 without reading all jobs from Next().
	err = s1.Close()
	require.NoError(t, err)

	// Open stream 2 and ensure all jobs are still available.
	s2, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-2",
		BufferSize: 5,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer s2.Close()

	for i := 1; i <= 3; i++ {
		select {
		case exec := <-s2.Next():
			require.NotNil(t, exec)
			err = exec.Complete(ctx)
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatalf("timeout waiting for job %d on stream 2", i)
		}
	}
}

func TestUniqueKeyPreservedOnLateComplete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d := inmemory.New()

	rec1 := &driver.Record{
		ID:        "job-first",
		Kind:      "kind",
		Queue:     "default",
		UniqueKey: "shared-key",
		RunAt:     time.Now().UTC(),
	}
	err := d.Enqueue(ctx, rec1)
	require.NoError(t, err)

	s, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-1",
		BufferSize: 1,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer s.Close()

	exec1 := <-s.Next()
	require.NotNil(t, exec1)

	// Complete first execution.
	err = exec1.Complete(ctx)
	require.NoError(t, err)

	// Enqueue second job with the same unique key.
	rec2 := &driver.Record{
		ID:        "job-second",
		Kind:      "kind",
		Queue:     "default",
		UniqueKey: "shared-key",
		RunAt:     time.Now().UTC(),
	}
	err = d.Enqueue(ctx, rec2)
	require.NoError(t, err)

	// Late complete on first execution should not remove the second job's unique key.
	err = exec1.Complete(ctx)
	require.NoError(t, err)

	// A third job with same key must be rejected as duplicate.
	rec3 := &driver.Record{
		ID:        "job-third",
		Kind:      "kind",
		Queue:     "default",
		UniqueKey: "shared-key",
		RunAt:     time.Now().UTC(),
	}
	err = d.Enqueue(ctx, rec3)
	require.NoError(t, err)

	exec2 := <-s.Next()
	require.NotNil(t, exec2)
	assert.Equal(t, "job-second", exec2.Record().ID)

	err = exec2.Complete(ctx)
	require.NoError(t, err)
}

func TestDeterministicFIFOOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d := inmemory.New()
	fixedRunAt := time.Now().UTC()
	baseEnqueuedAt := fixedRunAt

	for i := 1; i <= 5; i++ {
		err := d.Enqueue(ctx, &driver.Record{
			ID:         "job-" + time.Duration(i).String(),
			Kind:       "kind",
			Queue:      "default",
			RunAt:      fixedRunAt,
			EnqueuedAt: baseEnqueuedAt.Add(time.Duration(i) * time.Millisecond),
		})
		require.NoError(t, err)
	}

	s, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-1",
		BufferSize: 5,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer s.Close()

	for i := 1; i <= 5; i++ {
		select {
		case exec := <-s.Next():
			assert.Equal(t, "job-"+time.Duration(i).String(), exec.Record().ID)
			err = exec.Complete(ctx)
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatalf("timeout waiting for job %d", i)
		}
	}
}

func TestEnqueueDuplicateID(t *testing.T) {
	ctx := context.Background()
	d := inmemory.New()

	rec := &driver.Record{
		ID:    "job-dup-id",
		Kind:  "test-kind",
		Queue: "default",
	}

	err := d.Enqueue(ctx, rec)
	require.NoError(t, err)

	err = d.Enqueue(ctx, rec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate job ID")
}

func TestStaleExecutionLeaseIgnored(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d := inmemory.New()

	rec := &driver.Record{
		ID:    "job-lease",
		Kind:  "test-kind",
		Queue: "default",
		RunAt: time.Now().UTC(),
	}
	err := d.Enqueue(ctx, rec)
	require.NoError(t, err)

	s, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-1",
		BufferSize: 1,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer s.Close()

	// Claim attempt 0.
	exec1 := <-s.Next()
	require.NotNil(t, exec1)

	// Fail and reschedule.
	err = exec1.Fail(ctx, time.Now().UTC(), 1, assert.AnError)
	require.NoError(t, err)

	// Claim attempt 1.
	exec2 := <-s.Next()
	require.NotNil(t, exec2)
	assert.Equal(t, 1, exec2.Record().Attempt)

	// Stale exec1 operations must not mutate the active job.
	err = exec1.Complete(ctx)
	require.NoError(t, err)

	err = exec1.Snooze(ctx, time.Now().UTC().Add(time.Hour))
	require.NoError(t, err)

	err = exec1.Fail(ctx, time.Now().UTC().Add(time.Hour), 2, assert.AnError)
	require.NoError(t, err)

	err = exec1.Discard(ctx, assert.AnError)
	require.NoError(t, err)

	// exec2 must still be active and able to complete.
	err = exec2.Complete(ctx)
	require.NoError(t, err)
}

func TestBufferSizeLimitNoStarvation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d := inmemory.New()

	for i := 1; i <= 2; i++ {
		err := d.Enqueue(ctx, &driver.Record{
			ID:         "job-starve-" + time.Duration(i).String(),
			Kind:       "kind",
			Queue:      "default",
			RunAt:      time.Now().UTC(),
			EnqueuedAt: time.Now().UTC().Add(time.Duration(i) * time.Millisecond),
		})
		require.NoError(t, err)
	}

	// Stream 1 has buffer size 1 and will not consume its channel immediately.
	s1, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-1",
		BufferSize: 1,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer s1.Close()

	// Stream 2 also has buffer size 1.
	s2, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-2",
		BufferSize: 1,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer s2.Close()

	// Stream 2 must receive the second job because Stream 1 only claims 1 job.
	select {
	case exec2 := <-s2.Next():
		require.NotNil(t, exec2)
		assert.Equal(t, "job-starve-2ns", exec2.Record().ID)
		err = exec2.Complete(ctx)
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for stream 2 job")
	}

	// Now consume the first job from Stream 1.
	select {
	case exec1 := <-s1.Next():
		require.NotNil(t, exec1)
		assert.Equal(t, "job-starve-1ns", exec1.Record().ID)
		err = exec1.Complete(ctx)
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for stream 1 job")
	}
}

func TestSynchronousClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d := inmemory.New()

	err := d.Enqueue(ctx, &driver.Record{
		ID:    "job-sync-close",
		Kind:  "kind",
		Queue: "default",
		RunAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	s1, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-1",
		BufferSize: 1,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)

	// Close stream 1 synchronously.
	err = s1.Close()
	require.NoError(t, err)

	// Immediately start stream 2. The job must be pending right away.
	s2, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-2",
		BufferSize: 1,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer s2.Close()

	select {
	case exec := <-s2.Next():
		require.NotNil(t, exec)
		assert.Equal(t, "job-sync-close", exec.Record().ID)
		err = exec.Complete(ctx)
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for job on stream 2 after s1 close")
	}
}

func TestExecutionStateVerification(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d := inmemory.New()

	rec := &driver.Record{
		ID:    "job-state",
		Kind:  "kind",
		Queue: "default",
		RunAt: time.Now().UTC(),
	}
	err := d.Enqueue(ctx, rec)
	require.NoError(t, err)

	s, err := d.Stream(ctx, driver.StreamConfig{
		WorkerID:   "worker-1",
		BufferSize: 1,
		Queues:     []string{"default"},
	})
	require.NoError(t, err)
	defer s.Close()

	exec := <-s.Next()
	require.NotNil(t, exec)

	err = exec.Complete(ctx)
	require.NoError(t, err)

	// Calling Snooze, Fail, Discard, or Complete again should not fail or corrupt state.
	err = exec.Snooze(ctx, time.Now().UTC().Add(time.Hour))
	require.NoError(t, err)

	err = exec.Fail(ctx, time.Now().UTC().Add(time.Hour), 1, assert.AnError)
	require.NoError(t, err)

	err = exec.Discard(ctx, assert.AnError)
	require.NoError(t, err)

	err = exec.Complete(ctx)
	require.NoError(t, err)
}

func TestEndToEndWithWorkerAndEnqueuer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	drv := inmemory.New()
	enq := uppgift.NewEnqueuer(drv)
	mux := uppgift.NewMux()

	received := make(chan string, 1)
	mux.Register(uppgift.HandlerFunc[testJobArgs](func(ctx context.Context, job *uppgift.Job[testJobArgs]) error {
		received <- job.Args.Greeting
		return nil
	}))

	worker := uppgift.NewWorker(drv, mux, uppgift.WithConcurrency(2))

	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()

	go func() {
		_ = worker.Start(workerCtx)
	}()

	jobID, err := enq.Enqueue(ctx, testJobArgs{Greeting: "welcome"})
	require.NoError(t, err)
	require.NotEmpty(t, jobID)

	select {
	case msg := <-received:
		assert.Equal(t, "welcome", msg)
	case <-ctx.Done():
		t.Fatal("timeout waiting for worker to process job")
	}
}
