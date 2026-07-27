ALTER TABLE source_object
    ADD COLUMN IF NOT EXISTS processing_started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS processing_finished_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS processing_error_message TEXT,
    ADD COLUMN IF NOT EXISTS processing_attempt_count INTEGER NOT NULL DEFAULT 0;

UPDATE source_object
SET processing_started_at = COALESCE(processing_started_at, collected_at);

UPDATE source_object
SET processing_finished_at = COALESCE(processing_finished_at, collected_at)
WHERE processing_status IN ('PROCESSED', 'FAILED');

ALTER TABLE source_object
    ADD CONSTRAINT ck_source_object_processing_attempt_count
        CHECK (processing_attempt_count >= 0);

CREATE INDEX IF NOT EXISTS idx_source_object_processing_state
    ON source_object (processing_status, processing_started_at DESC);
