CREATE TABLE IF NOT EXISTS log_queue (
    id INTEGER PRIMARY KEY,

    run_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    event_id TEXT NOT NULL,

    mode TEXT NOT NULL
        CHECK (mode IN ('CENTRAL', 'AGENT')),

    payload_json TEXT NOT NULL,
    result_json TEXT,

    state TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (
            state IN (
                'PENDING',
                'PROCESSING',
                'RETRY_WAIT',
                'COMPLETED',
                'STORAGE_FAILED'
            )
        ),

    received_at_ms INTEGER NOT NULL,
    processing_started_at_ms INTEGER,

    attempt_count INTEGER NOT NULL DEFAULT 0
        CHECK (attempt_count >= 0),

    next_attempt_at_ms INTEGER NOT NULL DEFAULT 0,
    last_storage_error TEXT,

    UNIQUE(run_id, source_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_log_queue_ready
    ON log_queue (state, next_attempt_at_ms, id);

CREATE INDEX IF NOT EXISTS idx_log_queue_received
    ON log_queue (received_at_ms);

CREATE TABLE IF NOT EXISTS json_repair_job (
    id INTEGER PRIMARY KEY,
    run_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'PENDING',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at_ms INTEGER NOT NULL DEFAULT 0,
    claim_until_ms INTEGER,
    provider TEXT,
    target_id TEXT,
    model TEXT,
    prompt_version TEXT,
    reason TEXT,
    UNIQUE(run_id, source_id, event_id),
    FOREIGN KEY (run_id, source_id, event_id)
        REFERENCES log_queue (run_id, source_id, event_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_json_repair_job_ready
    ON json_repair_job (state, next_attempt_at_ms, id);

CREATE INDEX IF NOT EXISTS idx_json_repair_job_claim
    ON json_repair_job (claim_until_ms) WHERE state = 'PROCESSING';

CREATE TABLE IF NOT EXISTS json_repair_attempt (
    job_id INTEGER NOT NULL REFERENCES json_repair_job(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    outcome TEXT,
    reason TEXT,
    provider TEXT NOT NULL,
    target_id TEXT NOT NULL,
    model TEXT,
    prompt_version TEXT,
    model_decision TEXT,
    model_reason TEXT,
    response_text TEXT,
    candidate_json TEXT,
    syntax_valid INTEGER,
    values_preserved INTEGER,
    PRIMARY KEY (job_id, attempt_number)
);
