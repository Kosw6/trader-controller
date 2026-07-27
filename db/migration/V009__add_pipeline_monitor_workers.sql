-- V008에서 만든 managed worker control-plane에 운영 관측용 worker를 추가한다.
-- 기존 V008이 이미 적용된 DB에서도 Flyway checksum 충돌이 나지 않도록 별도 버전으로 둔다.
INSERT INTO pipeline_worker_control (name, display_name, worker_type, desired_state, config)
VALUES
    ('kafka-health', 'Kafka Health Checker', 'KAFKA_HEALTH', 'DISABLED', '{}'::jsonb),
    ('job-status-aggregator', 'Job Status Aggregator', 'JOB_STATUS_AGGREGATOR', 'DISABLED', '{}'::jsonb),
    ('consumer-lag-monitor', 'Consumer Lag Monitor', 'CONSUMER_LAG_MONITOR', 'DISABLED', '{}'::jsonb)
ON CONFLICT (name) DO NOTHING;

INSERT INTO pipeline_worker_runtime (name, runtime_state)
VALUES
    ('kafka-health', 'STOPPED'),
    ('job-status-aggregator', 'STOPPED'),
    ('consumer-lag-monitor', 'STOPPED')
ON CONFLICT (name) DO NOTHING;
