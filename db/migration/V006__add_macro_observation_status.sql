CREATE TABLE macro_observation_status (
    id BIGSERIAL PRIMARY KEY,
    series_id BIGINT NOT NULL REFERENCES macro_series(id) ON DELETE CASCADE,
    reference_period VARCHAR(20) NOT NULL,
    period_code VARCHAR(10) NOT NULL,
    period_name VARCHAR(30),
    status VARCHAR(30) NOT NULL CHECK (
        status IN ('PROVIDER_UNAVAILABLE', 'NON_NUMERIC')
    ),
    reason_code VARCHAR(100),
    reason_message TEXT,
    raw_value TEXT,
    footnotes JSONB,
    raw_object_uri TEXT,
    content_hash CHAR(64) NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    UNIQUE (series_id, reference_period, status)
);

CREATE INDEX idx_macro_observation_status_series_period
    ON macro_observation_status (series_id, reference_period);
