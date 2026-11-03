-- Opt-in coordinated-v1 only. No live work is adopted or replayed by migration.
CREATE TABLE pipeline_recovery_raw_intent (
    work_key text PRIMARY KEY REFERENCES pipeline_recovery_work(work_key),
    receipt_id text NOT NULL,
    item_id bigint NOT NULL REFERENCES pipeline_job_item(id),
    job_id bigint NOT NULL REFERENCES pipeline_job(id),
    sources jsonb NOT NULL CHECK (jsonb_typeof(sources)='array' AND jsonb_array_length(sources)>0),
    contract jsonb NOT NULL,
    outbox_id bigint UNIQUE REFERENCES pipeline_outbox(id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    dispatched_at timestamptz,
    FOREIGN KEY(work_key,receipt_id) REFERENCES pipeline_recovery_receipt(work_key,receipt_id)
);
