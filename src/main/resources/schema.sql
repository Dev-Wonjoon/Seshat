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