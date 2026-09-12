CREATE TABLE cache_invalidation_task (
    graph_id BIGINT PRIMARY KEY,
    generation VARCHAR(36) NOT NULL,
    status VARCHAR(16) NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMP,
    last_error VARCHAR(1000),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT chk_cache_invalidation_task_status
        CHECK (status IN ('PENDING', 'FAILED')),
    CONSTRAINT chk_cache_invalidation_task_retry_count
        CHECK (retry_count >= 0)
);

CREATE INDEX idx_cache_invalidation_task_due
    ON cache_invalidation_task (status, next_retry_at);

COMMENT ON TABLE cache_invalidation_task IS
    'Node 커밋 후 실패한 조회 캐시 무효화 작업';
COMMENT ON COLUMN cache_invalidation_task.generation IS
    '이전 재시도 작업자가 최신 작업을 완료 처리하지 못하게 하는 소유권 세대';
