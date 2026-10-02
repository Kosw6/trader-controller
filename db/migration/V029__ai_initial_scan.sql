-- 초기 실행은 운영 준비 명령만 생성한다. AI 서비스 계정은 public 실행을 수정할 수 없다.
CREATE TABLE ai_initial_run (
 id uuid PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 expires_at timestamptz NOT NULL DEFAULT clock_timestamp()+interval '24 hours',
 note_upper_id bigint NOT NULL, node_upper_id bigint NOT NULL,
 kafka_boundary jsonb NOT NULL,
 status varchar(16) NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE','COMPLETED','EXPIRED','ABORTED')),
 completion_hash char(64), completed_at timestamptz
);
CREATE TABLE ai.initial_progress (
 run_id uuid NOT NULL, source_type varchar(8) NOT NULL CHECK(source_type IN ('NOTE','NODE')),
 last_id bigint NOT NULL DEFAULT 0, next_cursor text, checkpoint text,
 completed boolean NOT NULL DEFAULT false,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(run_id,source_type)
);
CREATE TABLE ai.initial_run_job (
 run_id uuid NOT NULL,job_id uuid NOT NULL REFERENCES ai.index_job,
 PRIMARY KEY(run_id,job_id)
);
