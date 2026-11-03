-- Opt-in coordinator only. Existing Job/Item/price/financial tables are untouched.
CREATE TABLE pipeline_recovery_quota (
    scope_key text PRIMARY KEY,
    interval_ms bigint NOT NULL CHECK (interval_ms BETWEEN 0 AND 60000),
    next_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE pipeline_recovery_work (
    work_key text PRIMARY KEY,
    identity jsonb NOT NULL,
    policy jsonb NOT NULL,
    scope_key text NOT NULL REFERENCES pipeline_recovery_quota(scope_key),
    phase text NOT NULL DEFAULT 'FETCH_RAW' CHECK (phase IN ('FETCH_RAW','RAW_READY','ETL_READY','CANDIDATE','VERIFIED','COMPLETE')),
    state text NOT NULL DEFAULT 'READY' CHECK (state IN ('READY','LEASED','PAUSED','REVIEW','COMPLETE')),
    fence bigint NOT NULL DEFAULT 0 CHECK (fence>=0),
    owner text NOT NULL DEFAULT '',
    lease_until timestamptz,
    deadline timestamptz NOT NULL,
    claims bigint NOT NULL DEFAULT 0 CHECK (claims>=0),
    requests bigint NOT NULL DEFAULT 0 CHECK (requests>=0),
    retries bigint NOT NULL DEFAULT 0 CHECK (retries>=0 AND retries<=requests),
    heartbeat_at timestamptz,
    progress_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE pipeline_recovery_claim (
    work_key text NOT NULL REFERENCES pipeline_recovery_work(work_key),
    fence bigint NOT NULL,
    owner text NOT NULL,
    claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (work_key, fence)
);
CREATE TABLE pipeline_recovery_request (
    work_key text NOT NULL REFERENCES pipeline_recovery_work(work_key),
    request_id text NOT NULL,
    operation_key text NOT NULL,
    scope_key text NOT NULL REFERENCES pipeline_recovery_quota(scope_key),
    fence bigint NOT NULL,
    outcome text NOT NULL DEFAULT 'PENDING',
    reserved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    reported_at timestamptz,
    PRIMARY KEY (work_key, request_id)
);
-- An unknown in-flight HTTP call is NOT released on lease expiration.
CREATE UNIQUE INDEX pipeline_recovery_one_inflight ON pipeline_recovery_request(scope_key) WHERE outcome='PENDING';
CREATE TABLE pipeline_recovery_receipt (
    work_key text NOT NULL REFERENCES pipeline_recovery_work(work_key),
    receipt_id text NOT NULL,
    from_phase text NOT NULL,
    to_phase text NOT NULL,
    content_hash text NOT NULL,
    fence bigint NOT NULL,
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (work_key, receipt_id)
);
