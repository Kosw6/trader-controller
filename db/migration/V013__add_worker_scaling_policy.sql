-- External Python worker-node scaling policy.
-- V008 pipeline_worker_control/runtime manages Go in-process workers.
-- These tables are intentionally separate: they describe when the controller
-- should start/stop an external collector/ETL node such as a local Docker
-- container or an AWS Auto Scaling Group.

CREATE TABLE IF NOT EXISTS pipeline_worker_target (
    target_key VARCHAR(80) PRIMARY KEY,
    display_name VARCHAR(120) NOT NULL,
    environment VARCHAR(20) NOT NULL,
    driver VARCHAR(30) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    min_capacity INTEGER NOT NULL DEFAULT 0,
    max_capacity INTEGER NOT NULL DEFAULT 1,
    desired_capacity INTEGER NOT NULL DEFAULT 0,
    scale_in_idle_seconds INTEGER NOT NULL DEFAULT 900,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_pipeline_worker_target_environment
        CHECK (environment IN ('LOCAL', 'AWS')),
    CONSTRAINT ck_pipeline_worker_target_driver
        CHECK (driver IN ('LOCAL_DOCKER', 'AWS_ASG')),
    CONSTRAINT ck_pipeline_worker_target_capacity_range
        CHECK (
            min_capacity >= 0
            AND max_capacity >= min_capacity
            AND desired_capacity >= min_capacity
            AND desired_capacity <= max_capacity
        ),
    CONSTRAINT ck_pipeline_worker_target_idle
        CHECK (scale_in_idle_seconds >= 0)
);

CREATE TABLE IF NOT EXISTS pipeline_worker_policy (
    id BIGSERIAL PRIMARY KEY,
    policy_key VARCHAR(100) NOT NULL UNIQUE,
    target_key VARCHAR(80) NOT NULL REFERENCES pipeline_worker_target(target_key) ON DELETE CASCADE,
    source VARCHAR(30) NOT NULL,
    worker_name VARCHAR(100) NOT NULL,
    worker_role VARCHAR(30) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    mode VARCHAR(30) NOT NULL,
    topic VARCHAR(160),
    consumer_group VARCHAR(160),
    scale_out_lag_threshold BIGINT NOT NULL DEFAULT 1,
    scale_out_queue_threshold BIGINT NOT NULL DEFAULT 1,
    batch_size INTEGER NOT NULL DEFAULT 1,
    max_concurrency INTEGER NOT NULL DEFAULT 1,
    scale_in_idle_seconds INTEGER,
    allowed_start_time TIME,
    allowed_end_time TIME,
    check_interval_hours INTEGER,
    priority INTEGER NOT NULL DEFAULT 0,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_pipeline_worker_policy_source
        CHECK (source IN ('KIS', 'BLS', 'SEC', 'NEWS')),
    CONSTRAINT ck_pipeline_worker_policy_role
        CHECK (worker_role IN ('COLLECTOR', 'ETL', 'COMBINED')),
    CONSTRAINT ck_pipeline_worker_policy_mode
        CHECK (mode IN ('ALWAYS_ON', 'ON_DEMAND', 'SCHEDULED')),
    CONSTRAINT ck_pipeline_worker_policy_lag_threshold
        CHECK (scale_out_lag_threshold >= 0),
    CONSTRAINT ck_pipeline_worker_policy_queue_threshold
        CHECK (scale_out_queue_threshold >= 0),
    CONSTRAINT ck_pipeline_worker_policy_batch_size
        CHECK (batch_size > 0),
    CONSTRAINT ck_pipeline_worker_policy_concurrency
        CHECK (max_concurrency > 0),
    CONSTRAINT ck_pipeline_worker_policy_idle
        CHECK (scale_in_idle_seconds IS NULL OR scale_in_idle_seconds >= 0),
    CONSTRAINT ck_pipeline_worker_policy_check_interval
        CHECK (check_interval_hours IS NULL OR check_interval_hours > 0)
);

CREATE TABLE IF NOT EXISTS pipeline_worker_node_heartbeat (
    node_name VARCHAR(120) PRIMARY KEY,
    target_key VARCHAR(80) NOT NULL REFERENCES pipeline_worker_target(target_key) ON DELETE CASCADE,
    status VARCHAR(30) NOT NULL DEFAULT 'UNKNOWN',
    idle BOOLEAN NOT NULL DEFAULT TRUE,
    accepting_new_work BOOLEAN NOT NULL DEFAULT TRUE,
    running_workers JSONB NOT NULL DEFAULT '[]'::jsonb,
    running_job_items INTEGER NOT NULL DEFAULT 0,
    processing_source_objects INTEGER NOT NULL DEFAULT 0,
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_pipeline_worker_node_status
        CHECK (status IN ('STARTING', 'RUNNING', 'DRAINING', 'IDLE', 'STOPPING', 'STOPPED', 'ERROR', 'UNKNOWN')),
    CONSTRAINT ck_pipeline_worker_node_running_items
        CHECK (running_job_items >= 0),
    CONSTRAINT ck_pipeline_worker_node_processing_objects
        CHECK (processing_source_objects >= 0)
);

CREATE TABLE IF NOT EXISTS collection_planner_run (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(30) NOT NULL,
    planner_type VARCHAR(60) NOT NULL,
    status VARCHAR(30) NOT NULL,
    detected_count INTEGER NOT NULL DEFAULT 0,
    planned_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    reason VARCHAR(200),
    policy_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    pipeline_job_id BIGINT REFERENCES pipeline_job(id) ON DELETE SET NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_collection_planner_run_source
        CHECK (source IN ('KIS', 'BLS', 'SEC', 'NEWS')),
    CONSTRAINT ck_collection_planner_run_status
        CHECK (status IN ('DETECTED', 'NOOP', 'JOB_CREATED', 'SKIPPED', 'FAILED')),
    CONSTRAINT ck_collection_planner_run_counts
        CHECK (detected_count >= 0 AND planned_count >= 0 AND skipped_count >= 0)
);

CREATE TABLE IF NOT EXISTS pipeline_worker_scale_command (
    id BIGSERIAL PRIMARY KEY,
    target_key VARCHAR(80) NOT NULL REFERENCES pipeline_worker_target(target_key) ON DELETE CASCADE,
    command_type VARCHAR(30) NOT NULL,
    requested_capacity INTEGER NOT NULL,
    previous_capacity INTEGER,
    reason VARCHAR(100) NOT NULL,
    triggered_by VARCHAR(30) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'REQUESTED',
    planner_run_id BIGINT REFERENCES collection_planner_run(id) ON DELETE SET NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    CONSTRAINT ck_pipeline_worker_scale_command_type
        CHECK (command_type IN ('SET_CAPACITY', 'SCALE_OUT', 'SCALE_IN', 'DRAIN')),
    CONSTRAINT ck_pipeline_worker_scale_command_requested_capacity
        CHECK (requested_capacity >= 0),
    CONSTRAINT ck_pipeline_worker_scale_command_previous_capacity
        CHECK (previous_capacity IS NULL OR previous_capacity >= 0),
    CONSTRAINT ck_pipeline_worker_scale_command_triggered_by
        CHECK (triggered_by IN ('SYSTEM', 'MANUAL', 'SCHEDULER', 'PLANNER', 'LAG_MONITOR')),
    CONSTRAINT ck_pipeline_worker_scale_command_status
        CHECK (status IN ('REQUESTED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'SKIPPED'))
);

CREATE INDEX IF NOT EXISTS idx_pipeline_worker_policy_target
    ON pipeline_worker_policy (target_key, enabled, priority DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_worker_policy_source
    ON pipeline_worker_policy (source, worker_role, enabled);

CREATE INDEX IF NOT EXISTS idx_pipeline_worker_node_heartbeat_target
    ON pipeline_worker_node_heartbeat (target_key, last_heartbeat_at DESC);

CREATE INDEX IF NOT EXISTS idx_collection_planner_run_source
    ON collection_planner_run (source, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_collection_planner_run_status
    ON collection_planner_run (status, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_collection_planner_run_job
    ON collection_planner_run (pipeline_job_id)
    WHERE pipeline_job_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pipeline_worker_scale_command_target
    ON pipeline_worker_scale_command (target_key, requested_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_worker_scale_command_status
    ON pipeline_worker_scale_command (status, requested_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_worker_scale_command_planner_run
    ON pipeline_worker_scale_command (planner_run_id)
    WHERE planner_run_id IS NOT NULL;

INSERT INTO pipeline_worker_target (
    target_key,
    display_name,
    environment,
    driver,
    enabled,
    min_capacity,
    max_capacity,
    desired_capacity,
    scale_in_idle_seconds,
    config
)
VALUES (
    'python-worker-node',
    'Python Collector/ETL Worker Node',
    'LOCAL',
    'LOCAL_DOCKER',
    TRUE,
    0,
    1,
    0,
    900,
    '{
        "composeProject": "trader",
        "composeFile": "docker-compose.local-worker.yml",
        "serviceName": "trader-python-worker"
    }'::jsonb
)
ON CONFLICT (target_key) DO NOTHING;

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
VALUES
    (
        'kis-etl-policy',
        'python-worker-node',
        'KIS',
        'kis_price_etl_worker',
        'ETL',
        TRUE,
        'ON_DEMAND',
        'trader.raw.kis.ready',
        'trader-data-kis-etl',
        100,
        1,
        300,
        4,
        900,
        NULL,
        NULL,
        NULL,
        100,
        '{}'::jsonb
    ),
    (
        'bls-etl-policy',
        'python-worker-node',
        'BLS',
        'bls_macro_etl_worker',
        'ETL',
        TRUE,
        'ON_DEMAND',
        'trader.raw.bls.ready',
        'trader-data-bls-etl',
        1,
        1,
        20,
        1,
        300,
        NULL,
        NULL,
        NULL,
        80,
        '{}'::jsonb
    ),
    (
        'sec-etl-policy',
        'python-worker-node',
        'SEC',
        'sec_financial_etl_worker',
        'ETL',
        TRUE,
        'SCHEDULED',
        'trader.raw.sec.ready',
        'trader-data-sec-etl',
        10,
        1,
        20,
        1,
        1800,
        '01:00',
        '06:00',
        168,
        60,
        '{}'::jsonb
    ),
    (
        'news-etl-policy',
        'python-worker-node',
        'NEWS',
        'news_etl_worker',
        'ETL',
        TRUE,
        'ON_DEMAND',
        'trader.raw.news.ready',
        'trader-data-news-etl',
        5000,
        1,
        500,
        2,
        600,
        NULL,
        NULL,
        NULL,
        40,
        '{}'::jsonb
    )
ON CONFLICT (policy_key) DO NOTHING;
