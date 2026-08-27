-- SDA operational state schema. Analytical evidence and generated rows remain in Delta.
CREATE TABLE IF NOT EXISTS sda_schema_migrations (
    migration_id TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

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

CREATE TABLE IF NOT EXISTS sda_generation_requests (
    request_id TEXT PRIMARY KEY,
    requester_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    source_scope JSONB NOT NULL,
    generation_options JSONB NOT NULL DEFAULT '{}'::jsonb,
    target_catalog TEXT,
    target_schema TEXT,
    intended_use TEXT,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sda_generation_plans (
    plan_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL REFERENCES sda_generation_requests(request_id),
    run_id TEXT NOT NULL REFERENCES sda_runs(run_id),
    version INTEGER NOT NULL CHECK (version > 0),
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    supersedes_plan_id TEXT,
    UNIQUE(request_id, version)
);

CREATE INDEX IF NOT EXISTS sda_generation_plans_request_version
    ON sda_generation_plans(request_id, version);

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

-- Immutable workflow transition history; current status is only a projection.
CREATE TABLE IF NOT EXISTS sda_transition_events (
    event_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES sda_runs(run_id),
    from_status TEXT NOT NULL,
    to_status TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS sda_transition_events_run_time
    ON sda_transition_events(run_id, occurred_at, event_id);

CREATE TABLE IF NOT EXISTS sda_run_steps (
    step_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES sda_runs(run_id),
    step_name TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    status TEXT NOT NULL,
    input_artifacts JSONB NOT NULL DEFAULT '[]'::jsonb,
    output_artifacts JSONB NOT NULL DEFAULT '[]'::jsonb,
    warning_count INTEGER NOT NULL DEFAULT 0 CHECK (warning_count >= 0),
    error_code TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,
    UNIQUE(run_id, step_name)
);

CREATE INDEX IF NOT EXISTS sda_run_steps_run_order
    ON sda_run_steps(run_id, started_at, step_id);

CREATE TABLE IF NOT EXISTS sda_artifact_uses (
    use_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES sda_runs(run_id),
    artifact_id TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('produced', 'reused', 'invalidated', 'rejected')),
    compatibility_fingerprint TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS sda_artifact_uses_run_time
    ON sda_artifact_uses(run_id, recorded_at, use_id);
