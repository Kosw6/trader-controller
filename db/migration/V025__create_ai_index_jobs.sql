-- AI 전용 상태만 저장한다. 노트 본문/사용자 토큰을 작업 DB에 복제하지 않는다.
CREATE SCHEMA IF NOT EXISTS ai;
CREATE TABLE ai.source_index_state (
 source_type varchar(8) NOT NULL CHECK(source_type IN ('NOTE','NODE','EDGE')),
 source_id bigint NOT NULL CHECK(source_id>0),
 target_revision bigint NOT NULL CHECK(target_revision>0),
 next_generation bigint NOT NULL DEFAULT 1,
 deleted boolean NOT NULL DEFAULT false,
 active_generation bigint, active_attempt uuid,
 PRIMARY KEY(source_type,source_id)
);
CREATE TABLE ai.index_job (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 source_type varchar(8) NOT NULL, source_id bigint NOT NULL,
 source_revision bigint NOT NULL CHECK(source_revision>0),
 policy_version varchar(128) NOT NULL,
 generation bigint NOT NULL CHECK(generation>0),
 status varchar(16) NOT NULL DEFAULT 'PENDING'
   CHECK(status IN ('PENDING','RUNNING','RETRY_WAIT','SUCCEEDED','SUPERSEDED','FAILED')),
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0),
 lease_token uuid, lease_owner varchar(128), lease_until timestamptz,
 next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 finished_at timestamptz, error_code varchar(64),
 UNIQUE(source_type,source_id,source_revision,policy_version),
 FOREIGN KEY(source_type,source_id) REFERENCES ai.source_index_state
);
CREATE INDEX ai_index_job_pending ON ai.index_job(next_attempt_at,created_at)
 WHERE status IN ('PENDING','RETRY_WAIT','RUNNING');
CREATE TABLE ai.event_receipt (
 event_id uuid PRIMARY KEY,
 payload_hash char(64) NOT NULL,
 job_id uuid NOT NULL REFERENCES ai.index_job,
 received_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- 격리 원문은 저장하지 않는다. 토큰/본문이 잘못 들어온 메시지도 로그로 유출하지 않는다.
-- stream_epoch는 토픽 재생성마다 변경하는 운영 식별자다(이름 재사용 offset 충돌 방지).
CREATE TABLE ai.event_quarantine (
 stream_epoch varchar(128) NOT NULL, topic varchar(249) NOT NULL,
 partition_id integer NOT NULL, offset_id bigint NOT NULL,
 error_code varchar(64) NOT NULL, payload_hash char(64) NOT NULL,
 payload_bytes integer NOT NULL, received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(stream_epoch,topic,partition_id,offset_id)
);
-- Relay는 네트워크 대기 동안 DB 트랜잭션을 열어두지 않는다.
ALTER TABLE search_source_outbox ADD COLUMN claim_token uuid;
ALTER TABLE search_source_outbox ADD COLUMN claim_until timestamptz;
ALTER TABLE search_source_outbox ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp();
ALTER TABLE search_source_outbox ADD COLUMN publish_attempts integer NOT NULL DEFAULT 0;
ALTER TABLE search_source_outbox ADD COLUMN last_error_code varchar(64);
