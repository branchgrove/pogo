-- +goose Up
CREATE SCHEMA IF NOT EXISTS uppgift;

CREATE TABLE IF NOT EXISTS uppgift.jobs (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload BYTEA NOT NULL,
    attributes JSONB,
    state TEXT NOT NULL,
    attempt INT NOT NULL,
    max_retries INT NOT NULL,
    run_at TIMESTAMPTZ NOT NULL,
    timeout_ms BIGINT NOT NULL,
    unique_key TEXT,
    locked_by TEXT,
    locked_until TIMESTAMPTZ,
    enqueued_at TIMESTAMPTZ NOT NULL,
    last_error TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_unique_key_idx
    ON uppgift.jobs (unique_key)
    WHERE unique_key IS NOT NULL AND state IN ('available', 'running');

CREATE INDEX IF NOT EXISTS jobs_available_idx
    ON uppgift.jobs (queue, run_at, id)
    WHERE state = 'available';

CREATE INDEX IF NOT EXISTS jobs_running_idx
    ON uppgift.jobs (queue, locked_until, run_at, id)
    WHERE state = 'running';

CREATE INDEX IF NOT EXISTS jobs_discarded_idx
    ON uppgift.jobs (enqueued_at)
    WHERE state = 'discarded';

-- +goose Down
DROP TABLE IF EXISTS uppgift.jobs;
