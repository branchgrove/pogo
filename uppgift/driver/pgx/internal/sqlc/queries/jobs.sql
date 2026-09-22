-- name: EnqueueJobs :exec
INSERT INTO uppgift.jobs (
    id,
    queue,
    kind,
    payload,
    attributes,
    state,
    attempt,
    max_retries,
    run_at,
    timeout_ms,
    unique_key,
    enqueued_at
)
SELECT
    t.id,
    t.queue,
    t.kind,
    t.payload,
    t.attributes,
    'available',
    0,
    t.max_retries,
    t.run_at,
    t.timeout_ms,
    NULLIF(t.unique_key, ''),
    t.enqueued_at
FROM (
    SELECT
        unnest(@id::text[]) AS id,
        unnest(@queue::text[]) AS queue,
        unnest(@kind::text[]) AS kind,
        unnest(@payload::bytea[]) AS payload,
        unnest(@attributes::jsonb[]) AS attributes,
        unnest(@max_retries::int[]) AS max_retries,
        unnest(@run_at::timestamptz[]) AS run_at,
        unnest(@timeout_ms::bigint[]) AS timeout_ms,
        unnest(@unique_key::text[]) AS unique_key,
        unnest(@enqueued_at::timestamptz[]) AS enqueued_at
) t
ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND state IN ('available', 'running') DO NOTHING;

-- name: NotifyQueue :exec
SELECT pg_notify('uppgift_events', $1);

-- name: CompleteJob :execrows
DELETE FROM uppgift.jobs
WHERE id = @id AND locked_by = @locked_by AND state = 'running';

-- name: SnoozeJob :execrows
UPDATE uppgift.jobs
SET state = 'available',
    run_at = @run_at,
    locked_by = NULL,
    locked_until = NULL
WHERE id = @id AND locked_by = @locked_by AND state = 'running';

-- name: FailJob :execrows
UPDATE uppgift.jobs
SET state = 'available',
    run_at = @run_at,
    attempt = @attempt,
    last_error = @last_error,
    locked_by = NULL,
    locked_until = NULL
WHERE id = @id AND locked_by = @locked_by AND state = 'running';

-- name: DiscardJob :execrows
UPDATE uppgift.jobs
SET state = 'discarded',
    last_error = @last_error,
    locked_by = NULL,
    locked_until = NULL
WHERE id = @id AND locked_by = @locked_by AND state = 'running';

-- name: ClaimJobs :many
WITH claimable AS (
    SELECT id
    FROM uppgift.jobs
    WHERE (COALESCE(cardinality(@queues::text[]), 0) = 0 OR queue = ANY(@queues::text[]))
      AND (
        (state = 'available' AND run_at <= now())
        OR
        (state = 'running' AND locked_until < now())
      )
    ORDER BY
        (state = 'running') DESC,
        run_at ASC,
        id ASC
    LIMIT @claim_limit::int
    FOR UPDATE SKIP LOCKED
),
claimed AS (
    UPDATE uppgift.jobs j
    SET state = 'running',
        locked_by = @worker_id::text,
        locked_until = now() + (COALESCE(NULLIF(j.timeout_ms, 0), 300000) * interval '1 millisecond') + interval '30 seconds'
    FROM claimable
    WHERE j.id = claimable.id
    RETURNING j.id, j.kind, j.queue, j.payload, j.attributes, j.max_retries, j.attempt, j.timeout_ms, j.run_at, j.enqueued_at, j.unique_key
),
next_upcoming AS (
    SELECT MIN(t.next_time) AS next_run_at
    FROM (
        SELECT MIN(run_at) AS next_time
        FROM uppgift.jobs
        WHERE (COALESCE(cardinality(@queues::text[]), 0) = 0 OR queue = ANY(@queues::text[]))
          AND state = 'available'
          AND run_at > now()
        UNION ALL
        SELECT MIN(locked_until) AS next_time
        FROM uppgift.jobs
        WHERE (COALESCE(cardinality(@queues::text[]), 0) = 0 OR queue = ANY(@queues::text[]))
          AND state = 'running'
          AND locked_until > now()
    ) t
    WHERE (SELECT COUNT(*) FROM claimed) < @claim_limit::int
)
SELECT
    c.id,
    c.kind,
    c.queue,
    c.payload,
    c.attributes,
    c.max_retries,
    c.attempt,
    c.timeout_ms,
    c.run_at,
    c.enqueued_at,
    c.unique_key,
    u.next_run_at::timestamptz AS next_run_at
FROM next_upcoming u
FULL JOIN claimed c ON true;

-- name: ReleaseBuffer :exec
UPDATE uppgift.jobs
SET state = 'available',
    locked_by = NULL,
    locked_until = NULL
WHERE id = ANY(@ids::text[])
  AND locked_by = @worker_id::text
  AND state = 'running';

-- name: PurgeDiscardedJobs :execrows
DELETE FROM uppgift.jobs
WHERE state = 'discarded'
  AND enqueued_at < @before::timestamptz;
