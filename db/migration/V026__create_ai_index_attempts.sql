-- 실행별 외부 쓰기 격리. lease 토큰은 같은 generation 재선점에서도 새로 발급한다.
CREATE TABLE ai.index_attempt (
 attempt_id uuid PRIMARY KEY,
 job_id uuid NOT NULL REFERENCES ai.index_job,
 source_type varchar(8) NOT NULL, source_id bigint NOT NULL,
 generation bigint NOT NULL,
 status varchar(16) NOT NULL CHECK(status IN ('PREPARING','ACTIVE','RETIRED','REMOVED')),
 document_hash char(64), chunk_count integer,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 cleanup_after timestamptz,
 cleaned_at timestamptz
);
CREATE INDEX ai_attempt_cleanup ON ai.index_attempt(cleanup_after)
 WHERE status IN ('RETIRED','REMOVED');
ALTER TABLE ai.index_job ADD COLUMN started_at timestamptz;
ALTER TABLE ai.index_job ADD COLUMN budget_seconds double precision NOT NULL DEFAULT 0;
