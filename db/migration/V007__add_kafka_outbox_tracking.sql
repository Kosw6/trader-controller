-- Kafka relay가 Job Item 이벤트를 발행하고 Consumer 처리 여부를 추적하기 위한 보강이다.
-- PostgreSQL의 pipeline_outbox를 source of truth로 두고 Kafka는 delivery log로 사용한다.

-- JOB_ITEM_QUEUED 이벤트는 source_object가 생성되기 전에 발생할 수 있다.
-- 예: 관리자 Job 생성 -> pipeline_job_item 생성 -> outbox 이벤트 저장 -> 이후 Worker가 원본 수집.
ALTER TABLE pipeline_outbox
    ALTER COLUMN source_object_id DROP NOT NULL;

-- 기존 char(64) 해시 키 대신 사람이 읽을 수 있는 키를 저장한다.
-- 예: JOB_ITEM_QUEUED:31
ALTER TABLE pipeline_outbox
    ALTER COLUMN idempotency_key TYPE VARCHAR(255);

-- Kafka publish 성공 후 producer가 반환한 위치를 저장한다.
-- offset은 메시지 생존 여부 확인, 장애 분석, replay 진단에 사용한다.
ALTER TABLE pipeline_outbox
    ADD COLUMN IF NOT EXISTS kafka_partition INT,
    ADD COLUMN IF NOT EXISTS kafka_offset BIGINT;

CREATE INDEX IF NOT EXISTS idx_pipeline_outbox_kafka_offset
    ON pipeline_outbox (topic, kafka_partition, kafka_offset)
    WHERE kafka_partition IS NOT NULL
      AND kafka_offset IS NOT NULL;

-- Consumer별 중복 처리 방지 테이블이다.
-- 같은 Kafka 메시지가 재발행되거나 consumer rebalance 후 다시 전달되어도
-- consumer_name + idempotency_key 기준으로 이미 처리한 이벤트를 skip할 수 있다.
CREATE TABLE IF NOT EXISTS processed_event (
    consumer_name VARCHAR(100) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    topic VARCHAR(255),
    partition_no INT,
    offset_no BIGINT,
    status VARCHAR(30) NOT NULL,
    error_message TEXT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (consumer_name, idempotency_key),
    CONSTRAINT ck_processed_event_status CHECK (
        status IN ('SUCCESS', 'SKIPPED', 'FAILED')
    )
);

CREATE INDEX IF NOT EXISTS idx_processed_event_processed_at
    ON processed_event (processed_at DESC);
