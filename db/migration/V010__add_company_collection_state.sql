-- Company-level freshness state for provider APIs such as SEC.
-- This table is intentionally separate from normalized filing/financial tables:
-- it answers "when should this company be checked again?" before an expensive
-- external API call is made.

CREATE TABLE IF NOT EXISTS company_collection_state (
    id BIGSERIAL PRIMARY KEY,
    provider VARCHAR(30) NOT NULL,
    company_id BIGINT NOT NULL REFERENCES company(id) ON DELETE CASCADE,
    data_type VARCHAR(50) NOT NULL,
    last_checked_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_seen_external_id VARCHAR(255),
    next_check_at TIMESTAMPTZ,
    check_interval_hours INTEGER NOT NULL DEFAULT 168,
    priority INTEGER NOT NULL DEFAULT 0,
    last_status VARCHAR(30),
    last_error_code VARCHAR(100),
    last_error_message TEXT,
    checkpoint JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (provider, company_id, data_type),
    CONSTRAINT ck_company_collection_state_interval
        CHECK (check_interval_hours > 0),
    CONSTRAINT ck_company_collection_state_status
        CHECK (
            last_status IS NULL OR last_status IN (
                'SUCCESS',
                'SKIPPED',
                'FAILED',
                'NOT_DUE',
                'NO_CHANGE'
            )
        )
);

CREATE INDEX IF NOT EXISTS idx_company_collection_state_due
    ON company_collection_state (provider, data_type, next_check_at, priority DESC);

CREATE INDEX IF NOT EXISTS idx_company_collection_state_company
    ON company_collection_state (company_id, provider, data_type);
