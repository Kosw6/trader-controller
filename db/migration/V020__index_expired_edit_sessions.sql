-- DB fallback 편집 세션을 updated_at 기준으로 오래된 순서부터 정리한다.
-- edit_session은 trader-backend의 기존 Spring 스키마에 포함된 테이블이다.
CREATE INDEX IF NOT EXISTS idx_edit_session_updated_at
    ON edit_session (updated_at);
