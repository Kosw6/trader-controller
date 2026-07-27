-- Kafka consumer lag을 운영자가 UI에서 확인할 수 있도록 측정 스냅샷을 저장한다.
CREATE TABLE IF NOT EXISTS kafka_consumer_lag_snapshot (
    id BIGSERIAL PRIMARY KEY,
    topic VARCHAR(200) NOT NULL,
    consumer_group VARCHAR(200) NOT NULL,
    total_lag BIGINT NOT NULL,
    threshold_lag BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL,
    partition_lags JSONB NOT NULL DEFAULT '{}'::jsonb,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_kafka_consumer_lag_snapshot_status
        CHECK (status IN ('OK', 'WARN', 'CRITICAL'))
);

CREATE INDEX IF NOT EXISTS idx_kafka_consumer_lag_snapshot_latest
    ON kafka_consumer_lag_snapshot (topic, consumer_group, measured_at DESC);

