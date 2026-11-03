-- Opt-in explicit exact-scope promotion. Does not update legacy facts/mappings.
CREATE TABLE pipeline_financial_revision_approval (
 id bigserial PRIMARY KEY,
 scope_hash text NOT NULL,
 revision_id bigint NOT NULL REFERENCES pipeline_financial_revision(id),
 work_key text NOT NULL REFERENCES pipeline_recovery_work(work_key),
 previous_revision_id bigint REFERENCES pipeline_financial_revision(id),
 approval_hash text NOT NULL UNIQUE,
 public_fact jsonb NOT NULL,
 public_fact_hash text NOT NULL,
 approved_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE pipeline_financial_revision_active (
 scope_hash text PRIMARY KEY,
 company_id bigint NOT NULL REFERENCES company(id),
 metric_code text NOT NULL,
 period_start date,
 period_end date NOT NULL,
 revision_id bigint NOT NULL REFERENCES pipeline_financial_revision(id),
 approval_id bigint NOT NULL REFERENCES pipeline_financial_revision_approval(id)
);
