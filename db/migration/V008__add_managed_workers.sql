-- Go controller가 장기 실행 worker를 직접 관리하기 위한 control-plane 테이블이다.
-- desired_state는 관리자가 원하는 상태이고, runtime_state는 현재 프로세스가 보고한 실제 상태다.
CREATE TABLE IF NOT EXISTS pipeline_worker_control (
    name VARCHAR(80) PRIMARY KEY,
    display_name VARCHAR(120) NOT NULL,
    worker_type VARCHAR(40) NOT NULL,
    desired_state VARCHAR(20) NOT NULL DEFAULT 'DISABLED',
    run_once_requested_at TIMESTAMPTZ,
    run_once_claimed_at TIMESTAMPTZ,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_pipeline_worker_control_desired_state
        CHECK (desired_state IN ('ENABLED', 'DISABLED'))
);

CREATE TABLE IF NOT EXISTS pipeline_worker_runtime (
    name VARCHAR(80) PRIMARY KEY REFERENCES pipeline_worker_control(name) ON DELETE CASCADE,
    runtime_state VARCHAR(20) NOT NULL DEFAULT 'STOPPED',
    owner_instance_id VARCHAR(120),
    last_heartbeat_at TIMESTAMPTZ,
    last_started_at TIMESTAMPTZ,
    last_stopped_at TIMESTAMPTZ,
    last_run_at TIMESTAMPTZ,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_pipeline_worker_runtime_state
        CHECK (runtime_state IN ('STOPPED', 'STARTING', 'RUNNING', 'RUN_ONCE', 'STOPPING', 'ERROR'))
);

CREATE INDEX IF NOT EXISTS idx_pipeline_worker_runtime_heartbeat
    ON pipeline_worker_runtime (runtime_state, last_heartbeat_at);

-- 1차 관리 대상은 Kafka outbox relay다. 기본은 DISABLED로 두고,
-- 관리자 페이지에서 한 번 ENABLED로 바꾸면 controller 재시작 후에도 계속 실행된다.
INSERT INTO pipeline_worker_control (name, display_name, worker_type, desired_state, config)
VALUES ('outbox-relay', 'Kafka Outbox Relay', 'OUTBOX_RELAY', 'DISABLED', '{}'::jsonb)
ON CONFLICT (name) DO NOTHING;

INSERT INTO pipeline_worker_runtime (name, runtime_state)
VALUES ('outbox-relay', 'STOPPED')
ON CONFLICT (name) DO NOTHING;
