-- A worker node must consume trader.jobs.events before provider-specific raw
-- events can be produced. Allow the shared job consumer to participate in the
-- same lag-based scaling flow as the provider ETL workers.
ALTER TABLE pipeline_worker_policy
    DROP CONSTRAINT IF EXISTS ck_pipeline_worker_policy_source;

ALTER TABLE pipeline_worker_policy
    ADD CONSTRAINT ck_pipeline_worker_policy_source
        CHECK (source IN ('JOB', 'KIS', 'BLS', 'SEC', 'NEWS'));

INSERT INTO pipeline_worker_policy (
    policy_key,
    target_key,
    source,
    worker_name,
    worker_role,
    enabled,
    mode,
    topic,
    consumer_group,
    scale_out_lag_threshold,
    scale_out_queue_threshold,
    batch_size,
    max_concurrency,
    scale_in_idle_seconds,
    allowed_start_time,
    allowed_end_time,
    check_interval_hours,
    priority,
    config
)
VALUES (
    'job-event-worker-policy',
    'python-worker-node',
    'JOB',
    'kafka_pipeline_consumer',
    'COMBINED',
    TRUE,
    'ON_DEMAND',
    'trader.jobs.events',
    'trader-data-workers',
    1,
    1,
    1,
    1,
    NULL,
    NULL,
    NULL,
    NULL,
    120,
    '{}'::jsonb
)
ON CONFLICT (policy_key) DO NOTHING;
