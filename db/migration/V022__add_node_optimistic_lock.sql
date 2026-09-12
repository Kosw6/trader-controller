-- 업무 version과 저장 경쟁 검출용 기술 버전을 분리한다.
ALTER TABLE node
    ADD COLUMN IF NOT EXISTS lock_version BIGINT NOT NULL DEFAULT 0;

-- 하나의 Node 업무 version에는 하나의 변경 이력만 존재해야 한다.
CREATE UNIQUE INDEX IF NOT EXISTS uq_node_history_node_version
    ON node_history (node_id, version);

COMMENT ON COLUMN node.lock_version IS
    'JPA 낙관적 잠금용 기술 버전. 사용자에게 노출되는 업무 version과 분리한다';
