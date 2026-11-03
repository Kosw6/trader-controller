-- Opt-in immutable normalization store. No changes to live fact/mapping rows.
ALTER TABLE pipeline_recovery_candidate DROP CONSTRAINT pipeline_recovery_candidate_kind_check;
ALTER TABLE pipeline_recovery_candidate ADD CONSTRAINT pipeline_recovery_candidate_kind_check
 CHECK(kind IN ('PRICE','FINANCIAL','FINANCIAL_REVISION'));
CREATE TABLE pipeline_financial_revision (
 id bigserial PRIMARY KEY,
 company_id bigint NOT NULL REFERENCES company(id),
 revision_hash text NOT NULL UNIQUE,
 scope jsonb NOT NULL,
 revision jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE pipeline_financial_revision_origin (
 work_key text NOT NULL REFERENCES pipeline_recovery_work(work_key),
 revision_id bigint NOT NULL REFERENCES pipeline_financial_revision(id),
 source_object_id bigint NOT NULL REFERENCES source_object(id),
 source_hash text NOT NULL,
 official_hash text NOT NULL,
 PRIMARY KEY(work_key,revision_id)
);
