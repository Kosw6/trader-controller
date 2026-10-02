-- 꺼진 AI 프로세스가 아닌 상시 백엔드에 알림 대상을 보관한다.
CREATE TABLE ai_wake_request (
 user_id bigint PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 notified_at timestamptz
);
