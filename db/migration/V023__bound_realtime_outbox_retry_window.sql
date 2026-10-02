-- 최초 시도 후 3분으로 자동 재처리 기한을 고정한다. Kafka 복구는 기한을 연장하지 않는다.
ALTER TABLE realtime_outbox
    ADD COLUMN first_attempt_at TIMESTAMP,
    ADD COLUMN retry_deadline_at TIMESTAMP,
    ADD COLUMN next_attempt_at TIMESTAMP,
    ADD COLUMN total_attempt_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN last_failure_code VARCHAR(50),
    ADD COLUMN kafka_recovery_pending BOOLEAN NOT NULL DEFAULT FALSE;

-- 과거 최초 시도 시각을 복원할 수 없으므로 created_at을 보수적으로 사용한다.
-- 배포 시각을 기산점으로 삼아 기존 실패 작업에 새 3분을 부여하지 않는다.
UPDATE realtime_outbox
   SET total_attempt_count = retry_count,
       first_attempt_at = CASE WHEN retry_count > 0 OR last_tried_at IS NOT NULL THEN created_at END,
       retry_deadline_at = CASE WHEN retry_count > 0 OR last_tried_at IS NOT NULL
                               THEN created_at + INTERVAL '3 minutes' END,
       last_failure_code = CASE
           WHEN last_error_message LIKE 'PUBLISH_UNAVAILABLE:%' THEN 'PUBLISH_UNAVAILABLE'
           WHEN last_error_message LIKE 'SENT_RECORD_FAILED:%' THEN 'SENT_RECORD_FAILED'
           WHEN last_error_message LIKE 'INVALID_EVENT:%' THEN 'INVALID_EVENT'
           WHEN last_error_message LIKE 'PUBLISH_PERMANENT:%' THEN 'PUBLISH_PERMANENT'
           WHEN last_error_message IS NOT NULL THEN 'UNKNOWN' END,
       kafka_recovery_pending = (status = 'PENDING'
           AND coalesce(last_error_message LIKE 'PUBLISH_UNAVAILABLE:%', FALSE));

CREATE INDEX idx_realtime_outbox_due ON realtime_outbox (status, next_attempt_at, created_at);
CREATE INDEX idx_realtime_outbox_deadline ON realtime_outbox (status, retry_deadline_at);

ALTER TABLE realtime_outbox ADD CONSTRAINT ck_realtime_outbox_retry_window
    CHECK ((first_attempt_at IS NULL AND retry_deadline_at IS NULL)
        OR (first_attempt_at IS NOT NULL AND retry_deadline_at IS NOT NULL
            AND retry_deadline_at = first_attempt_at + INTERVAL '3 minutes'));

COMMENT ON COLUMN realtime_outbox.retry_count IS '현재 복구 구간의 선점 시도 수. 최초 시도 포함';
COMMENT ON COLUMN realtime_outbox.total_attempt_count IS 'Kafka 복구 시 초기화하지 않는 누적 선점 시도 수';
COMMENT ON COLUMN realtime_outbox.retry_deadline_at IS '최초 선점 + 3분. 재시도 및 Kafka 복구로 연장하지 않음';
