-- SDA operational state schema. Analytical evidence and generated rows remain in Delta.
CREATE TABLE IF NOT EXISTS sda_runs (
    run_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    plan_id TEXT,
    plan_fingerprint TEXT,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT sda_runs_status CHECK (status IN (
        'requested', 'planned', 'awaiting_approval', 'approved', 'executing',
        'generated_awaiting_validation', 'validated', 'privacy_approved',
        'published', 'rejected', 'cancelled', 'failed'
    )),
    CONSTRAINT sda_runs_version_nonnegative CHECK (version >= 0)
);

CREATE TABLE IF NOT EXISTS sda_execution_attempts (
    attempt_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES sda_runs(run_id),
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    worker_id TEXT,
    lease_expires_at TIMESTAMPTZ,
    retry_number INTEGER NOT NULL DEFAULT 0,
    error_code TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    CONSTRAINT sda_attempt_status CHECK (status IN ('running', 'succeeded', 'failed', 'abandoned')),
    CONSTRAINT sda_attempt_retry_nonnegative CHECK (retry_number >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS sda_one_running_attempt_per_stage
    ON sda_execution_attempts(run_id, stage) WHERE status = 'running';

CREATE TABLE IF NOT EXISTS sda_approvals (
    run_id TEXT NOT NULL REFERENCES sda_runs(run_id),
    approval_type TEXT NOT NULL,
    decision TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    decided_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, approval_type),
    CONSTRAINT sda_approval_decision CHECK (decision IN ('approved', 'rejected'))
);

-- Immutable user corrections/review notes; historical evidence is never mutated.
CREATE TABLE IF NOT EXISTS sda_feedback (
    feedback_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES sda_runs(run_id),
    actor TEXT NOT NULL,
    category TEXT NOT NULL,
    message TEXT NOT NULL,
    evidence_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT sda_feedback_identity_nonempty CHECK (
        length(trim(feedback_id)) > 0
        AND length(trim(actor)) > 0
        AND length(trim(category)) > 0
        AND length(trim(message)) > 0
    )
);

CREATE INDEX IF NOT EXISTS sda_feedback_run_time
    ON sda_feedback(run_id, created_at, feedback_id);

CREATE TABLE IF NOT EXISTS sda_audit_events (
    event_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES sda_runs(run_id),
    event_type TEXT NOT NULL,
    level TEXT NOT NULL,
    stage TEXT NOT NULL DEFAULT '',
    message TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT sda_audit_level CHECK (level IN ('INFO', 'WARNING', 'ERROR'))
);

CREATE INDEX IF NOT EXISTS sda_audit_events_run_time
    ON sda_audit_events(run_id, occurred_at);
