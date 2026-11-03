-- Candidate/verification evidence, not a substitute for service price/financial rows.
ALTER TABLE pipeline_recovery_work DROP CONSTRAINT pipeline_recovery_work_phase_check;
ALTER TABLE pipeline_recovery_work ADD CONSTRAINT pipeline_recovery_work_phase_check
 CHECK(phase IN ('FETCH_RAW','RAW_READY','ETL_READY','CANDIDATE','VERIFIED','PUBLISHED','COMPLETE'));
CREATE TABLE pipeline_recovery_candidate (
    work_key text PRIMARY KEY REFERENCES pipeline_recovery_work(work_key),
    kind text NOT NULL CHECK(kind IN ('PRICE','FINANCIAL')),
    plan jsonb NOT NULL,
    plan_hash text NOT NULL,
    content jsonb NOT NULL,
    content_hash text NOT NULL,
    published_result jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE pipeline_recovery_verification (
    work_key text PRIMARY KEY REFERENCES pipeline_recovery_candidate(work_key),
    plan_hash text NOT NULL,
    content_hash text NOT NULL,
    evidence jsonb NOT NULL,
    verified_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
