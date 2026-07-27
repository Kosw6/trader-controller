-- Add provider-aware asset identity, auditable company matching, and operational constraints.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE asset_alias (
    id BIGSERIAL PRIMARY KEY,
    asset_id BIGINT NOT NULL REFERENCES asset(id) ON DELETE CASCADE,
    alias VARCHAR(255) NOT NULL,
    normalized_alias VARCHAR(255) NOT NULL,
    alias_type VARCHAR(30) NOT NULL,
    language VARCHAR(10),
    source VARCHAR(50) NOT NULL,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (asset_id, normalized_alias, alias_type)
);

CREATE INDEX idx_asset_alias_normalized_trgm
    ON asset_alias USING GIN (normalized_alias gin_trgm_ops);

CREATE TABLE asset_provider_symbol (
    id BIGSERIAL PRIMARY KEY,
    asset_id BIGINT NOT NULL REFERENCES asset(id) ON DELETE CASCADE,
    provider VARCHAR(30) NOT NULL,
    provider_exchange_code VARCHAR(30) NOT NULL,
    provider_symbol VARCHAR(64) NOT NULL,
    source_record_id VARCHAR(100),
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    collected_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE NULLS NOT DISTINCT (
        asset_id,
        provider,
        provider_exchange_code,
        provider_symbol,
        source_record_id
    )
);

CREATE UNIQUE INDEX uq_asset_provider_symbol_current_identity
    ON asset_provider_symbol (provider, provider_exchange_code, provider_symbol)
    WHERE is_current = TRUE;

CREATE UNIQUE INDEX uq_asset_provider_symbol_current_per_provider
    ON asset_provider_symbol (asset_id, provider)
    WHERE is_current = TRUE;

CREATE TABLE asset_company_match (
    id BIGSERIAL PRIMARY KEY,
    asset_id BIGINT NOT NULL REFERENCES asset(id) ON DELETE CASCADE,
    company_id BIGINT NOT NULL REFERENCES company(id) ON DELETE CASCADE,
    match_method VARCHAR(50) NOT NULL,
    match_confidence NUMERIC(7, 6) NOT NULL,
    status VARCHAR(30) NOT NULL,
    sec_source_object_id BIGINT REFERENCES source_object(id) ON DELETE RESTRICT,
    market_source_object_id BIGINT REFERENCES source_object(id) ON DELETE RESTRICT,
    evidence JSONB NOT NULL DEFAULT '{}'::JSONB,
    matched_at TIMESTAMPTZ NOT NULL,
    reviewed_at TIMESTAMPTZ,
    reviewed_by VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_asset_company_match_confidence
        CHECK (match_confidence >= 0 AND match_confidence <= 1),
    CONSTRAINT ck_asset_company_match_status
        CHECK (status IN ('CANDIDATE', 'CONFIRMED', 'REVIEW_REQUIRED', 'REJECTED')),
    UNIQUE (asset_id, company_id)
);

CREATE UNIQUE INDEX uq_asset_company_match_confirmed
    ON asset_company_match (asset_id)
    WHERE status = 'CONFIRMED';

CREATE INDEX idx_asset_company_match_company_status
    ON asset_company_match (company_id, status);

CREATE UNIQUE INDEX uq_asset_symbol_history_current
    ON asset_symbol_history (asset_id)
    WHERE is_current = TRUE;

CREATE UNIQUE INDEX uq_macro_observation_vintage_initial
    ON macro_observation_vintage (observation_id)
    WHERE is_initial = TRUE;

CREATE UNIQUE INDEX uq_financial_metric_mapping_identity
    ON financial_metric_mapping (
        taxonomy,
        source_tag,
        company_id,
        valid_from
    ) NULLS NOT DISTINCT;

ALTER TABLE pipeline_job
    ADD CONSTRAINT ck_pipeline_job_status
        CHECK (status IN (
            'SCHEDULED', 'QUEUED', 'RUNNING', 'RETRY_WAIT',
            'CANCEL_REQUESTED', 'CANCELLED',
            'SUCCESS', 'PARTIAL_SUCCESS', 'FAILED'
        )),
    ADD CONSTRAINT ck_pipeline_job_retry_count
        CHECK (retry_count >= 0);

ALTER TABLE pipeline_job_item
    ADD CONSTRAINT ck_pipeline_job_item_status
        CHECK (status IN (
            'SCHEDULED', 'QUEUED', 'RUNNING', 'RETRY_WAIT',
            'COLLECTED', 'SUCCESS', 'FAILED', 'CANCELLED'
        )),
    ADD CONSTRAINT ck_pipeline_job_item_attempt_count
        CHECK (attempt_count >= 0);

ALTER TABLE source_object
    ADD CONSTRAINT ck_source_object_processing_status
        CHECK (processing_status IN ('COLLECTED', 'PROCESSING', 'PROCESSED', 'FAILED')),
    ADD CONSTRAINT ck_source_object_size
        CHECK (object_size >= 0);

ALTER TABLE pipeline_outbox
    ADD CONSTRAINT ck_pipeline_outbox_attempt_count
        CHECK (attempt_count >= 0);

ALTER TABLE data_coverage
    ADD CONSTRAINT ck_data_coverage_status
        CHECK (status IN ('COMPLETE', 'PARTIAL', 'MISSING', 'NOT_EXPECTED')),
    ADD CONSTRAINT ck_data_coverage_counts
        CHECK (expected_count >= 0 AND actual_count >= 0),
    ADD CONSTRAINT ck_data_coverage_period
        CHECK (period_end >= period_start);

ALTER TABLE data_gap
    ADD CONSTRAINT ck_data_gap_status
        CHECK (status IN ('OPEN', 'BACKFILL_SCHEDULED', 'RESOLVED')),
    ADD CONSTRAINT ck_data_gap_resolution_time
        CHECK (resolved_at IS NULL OR resolved_at >= detected_at);

ALTER TABLE news_company_link
    ADD CONSTRAINT ck_news_company_link_confidence
        CHECK (match_confidence >= 0 AND match_confidence <= 1),
    ADD CONSTRAINT ck_news_company_link_relevance
        CHECK (relevance_score >= 0 AND relevance_score <= 1),
    ADD CONSTRAINT ck_news_company_link_mention_count
        CHECK (mention_count >= 0);

ALTER TABLE news_asset_link
    ADD CONSTRAINT ck_news_asset_link_confidence
        CHECK (match_confidence >= 0 AND match_confidence <= 1),
    ADD CONSTRAINT ck_news_asset_link_relevance
        CHECK (relevance_score >= 0 AND relevance_score <= 1);

ALTER TABLE asset
    ADD CONSTRAINT ck_asset_listing_period
        CHECK (delisted_at IS NULL OR listed_at IS NULL OR delisted_at >= listed_at);

ALTER TABLE asset_symbol_history
    ADD CONSTRAINT ck_asset_symbol_history_period
        CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from);

ALTER TABLE company_alias
    ADD CONSTRAINT ck_company_alias_period
        CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from);

ALTER TABLE company_industry
    ADD CONSTRAINT ck_company_industry_period
        CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from);

ALTER TABLE financial_metric_mapping
    ADD CONSTRAINT ck_financial_metric_mapping_period
        CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from);

ALTER TABLE financial_fact
    ADD CONSTRAINT ck_financial_fact_period
        CHECK (period_start IS NULL OR period_end >= period_start);

CREATE INDEX idx_asset_company_id
    ON asset (company_id);

CREATE INDEX idx_industry_classification_parent_id
    ON industry_classification (parent_id);

CREATE INDEX idx_company_industry_classification_id
    ON company_industry (classification_id);

CREATE INDEX idx_price_bar_asset_time
    ON price_bar (asset_id, bar_time DESC);

CREATE INDEX idx_filing_company_date
    ON filing (company_id, filing_date DESC);

CREATE INDEX idx_financial_fact_company_period
    ON financial_fact (company_id, period_end DESC);

CREATE INDEX idx_financial_fact_filing_id
    ON financial_fact (filing_id);

CREATE INDEX idx_financial_fact_metric_id
    ON financial_fact (metric_id);

CREATE INDEX idx_financial_metric_mapping_metric_id
    ON financial_metric_mapping (metric_id);

CREATE INDEX idx_financial_metric_mapping_company_id
    ON financial_metric_mapping (company_id);

CREATE INDEX idx_news_article_story_cluster_id
    ON news_article (story_cluster_id);

CREATE INDEX idx_news_article_version_source_object_id
    ON news_article_version (source_object_id);

CREATE INDEX idx_news_company_link_company_id
    ON news_company_link (company_id, article_id);

CREATE INDEX idx_news_asset_link_asset_id
    ON news_asset_link (asset_id, article_id);

CREATE INDEX idx_macro_release_type_scheduled
    ON macro_release (release_type, scheduled_at DESC);

CREATE INDEX idx_macro_observation_release_id
    ON macro_observation (release_id);

CREATE INDEX idx_macro_observation_vintage_release_id
    ON macro_observation_vintage (release_id);

CREATE INDEX idx_pipeline_job_schedule_id
    ON pipeline_job (schedule_id);

CREATE INDEX idx_pipeline_job_status_scheduled
    ON pipeline_job (status, scheduled_at, priority DESC);

CREATE INDEX idx_pipeline_job_item_status
    ON pipeline_job_item (status, id);

CREATE INDEX idx_source_object_pipeline_job_item_id
    ON source_object (pipeline_job_item_id);

CREATE INDEX idx_pipeline_outbox_source_object_id
    ON pipeline_outbox (source_object_id);

CREATE INDEX idx_pipeline_outbox_job_item_id
    ON pipeline_outbox (job_item_id);

CREATE INDEX idx_data_coverage_resolution_job_id
    ON data_coverage (resolution_job_id);

CREATE INDEX idx_data_gap_resolution_job_id
    ON data_gap (resolution_job_id);

CREATE INDEX idx_record_lineage_target
    ON record_lineage (target_table, target_record_key);

CREATE INDEX idx_record_lineage_pipeline_job_id
    ON record_lineage (pipeline_job_id);
