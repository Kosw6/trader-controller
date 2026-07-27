-- Preserve imperfect provider bars for flow-oriented charts while keeping
-- the data-quality decision auditable and queryable.

ALTER TABLE price_bar
    ADD COLUMN quality_status VARCHAR(20) NOT NULL DEFAULT 'VALID',
    ADD COLUMN display_policy VARCHAR(20) NOT NULL DEFAULT 'FULL_OHLC',
    ADD COLUMN quality_checked_at TIMESTAMPTZ;

ALTER TABLE price_bar
    ADD CONSTRAINT ck_price_bar_quality_status
        CHECK (quality_status IN ('VALID', 'PARTIAL', 'INVALID', 'UNVERIFIED')),
    ADD CONSTRAINT ck_price_bar_display_policy
        CHECK (display_policy IN ('FULL_OHLC', 'CLOSE_ONLY', 'HIDDEN')),
    ADD CONSTRAINT ck_price_bar_quality_display_policy
        CHECK (
            (quality_status = 'VALID' AND display_policy = 'FULL_OHLC')
            OR (quality_status = 'PARTIAL' AND display_policy = 'CLOSE_ONLY')
            OR (quality_status IN ('INVALID', 'UNVERIFIED') AND display_policy = 'HIDDEN')
        );

CREATE TABLE price_bar_quality_issue (
    id BIGSERIAL PRIMARY KEY,
    price_bar_id BIGINT NOT NULL,
    asset_id BIGINT NOT NULL,
    source VARCHAR(30) NOT NULL,
    "interval" VARCHAR(8) NOT NULL,
    bar_time TIMESTAMPTZ NOT NULL,
    issue_code VARCHAR(50) NOT NULL,
    field_name VARCHAR(30) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    observed_value NUMERIC(24, 8),
    issue_status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
    detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMPTZ,
    resolution_source VARCHAR(30),
    resolution_note TEXT,
    details JSONB NOT NULL DEFAULT '{}'::JSONB,
    CONSTRAINT fk_price_bar_quality_issue_price_bar
        FOREIGN KEY (price_bar_id, asset_id, bar_time)
        REFERENCES price_bar (id, asset_id, bar_time)
        ON DELETE CASCADE,
    CONSTRAINT fk_price_bar_quality_issue_asset
        FOREIGN KEY (asset_id)
        REFERENCES asset (id)
        ON DELETE CASCADE,
    CONSTRAINT ck_price_bar_quality_issue_severity
        CHECK (severity IN ('INFO', 'WARNING', 'ERROR')),
    CONSTRAINT ck_price_bar_quality_issue_status
        CHECK (issue_status IN ('OPEN', 'RESOLVED', 'IGNORED')),
    CONSTRAINT ck_price_bar_quality_issue_resolution_time
        CHECK (resolved_at IS NULL OR resolved_at >= detected_at),
    CONSTRAINT ck_price_bar_quality_issue_resolution_status
        CHECK (
            (issue_status = 'RESOLVED' AND resolved_at IS NOT NULL)
            OR (issue_status <> 'RESOLVED' AND resolved_at IS NULL)
        ),
    UNIQUE NULLS NOT DISTINCT (
        asset_id,
        source,
        "interval",
        bar_time,
        issue_code,
        field_name
    )
);

CREATE INDEX idx_price_bar_quality_status
    ON price_bar (quality_status, display_policy, asset_id, "interval", bar_time DESC);

CREATE INDEX idx_price_bar_quality_issue_asset_time
    ON price_bar_quality_issue (asset_id, bar_time DESC);

CREATE INDEX idx_price_bar_quality_issue_status
    ON price_bar_quality_issue (issue_status, severity, detected_at DESC);

-- The legacy stock table currently has no missing OHLC and no negative volume
-- among invalid rows. If that changes, a separate policy is needed because
-- price_bar keeps OHLC fields NOT NULL.
DO $$
DECLARE
    unsupported_count BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO unsupported_count
    FROM stock s
    JOIN asset_provider_symbol aps
      ON aps.provider = 'KIS'
     AND aps.provider_symbol = s.symb
     AND aps.is_current = TRUE
    WHERE NOT (
        s.open IS NOT NULL
        AND s.high IS NOT NULL
        AND s.low IS NOT NULL
        AND s.close IS NOT NULL
        AND s.high >= GREATEST(s.open, s.close, s.low)
        AND s.low <= LEAST(s.open, s.close, s.high)
        AND (s.volume IS NULL OR s.volume >= 0)
    )
      AND (
        s.open IS NULL
        OR s.high IS NULL
        OR s.low IS NULL
        OR s.close IS NULL
        OR s.volume < 0
        OR aps.asset_id IS NULL
    );

    IF unsupported_count > 0 THEN
        RAISE EXCEPTION
            'unsupported legacy invalid price rows need manual policy before V004 backfill: %',
            unsupported_count;
    END IF;
END $$;

CREATE TEMP TABLE tmp_invalid_price_bar_stage
ON COMMIT DROP
AS
SELECT
    aps.asset_id,
    'KIS'::VARCHAR(30) AS source,
    '1D'::VARCHAR(8) AS "interval",
    s.timestamp AS bar_time,
    s.open::NUMERIC(24, 8) AS open,
    s.high::NUMERIC(24, 8) AS high,
    s.low::NUMERIC(24, 8) AS low,
    s.close::NUMERIC(24, 8) AS close,
    NULL::NUMERIC(24, 8) AS adjusted_close,
    s.volume::NUMERIC(30, 0) AS volume,
    'USD'::VARCHAR(3) AS currency,
    CASE
        WHEN s.high < GREATEST(s.open, s.close, s.low)
         AND s.low > LEAST(s.open, s.close, s.high)
            THEN 'INVALID'
        ELSE 'PARTIAL'
    END::VARCHAR(20) AS quality_status,
    CASE
        WHEN s.high < GREATEST(s.open, s.close, s.low)
         AND s.low > LEAST(s.open, s.close, s.high)
            THEN 'HIDDEN'
        ELSE 'CLOSE_ONLY'
    END::VARCHAR(20) AS display_policy
FROM stock s
JOIN asset_provider_symbol aps
  ON aps.provider = 'KIS'
 AND aps.provider_symbol = s.symb
 AND aps.is_current = TRUE
WHERE s.open IS NOT NULL
  AND s.high IS NOT NULL
  AND s.low IS NOT NULL
  AND s.close IS NOT NULL
  AND (s.volume IS NULL OR s.volume >= 0)
  AND NOT (
        s.high >= GREATEST(s.open, s.close, s.low)
    AND s.low <= LEAST(s.open, s.close, s.high)
  );

INSERT INTO price_bar (
    asset_id,
    source,
    "interval",
    bar_time,
    open,
    high,
    low,
    close,
    adjusted_close,
    volume,
    currency,
    collected_at,
    content_hash,
    quality_status,
    display_policy,
    quality_checked_at
)
SELECT
    asset_id,
    source,
    "interval",
    bar_time,
    open,
    high,
    low,
    close,
    adjusted_close,
    volume,
    currency,
    CURRENT_TIMESTAMP,
    NULL,
    quality_status,
    display_policy,
    CURRENT_TIMESTAMP
FROM tmp_invalid_price_bar_stage
ON CONFLICT (asset_id, source, "interval", bar_time)
DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    adjusted_close = EXCLUDED.adjusted_close,
    volume = EXCLUDED.volume,
    currency = EXCLUDED.currency,
    collected_at = EXCLUDED.collected_at,
    quality_status = EXCLUDED.quality_status,
    display_policy = EXCLUDED.display_policy,
    quality_checked_at = EXCLUDED.quality_checked_at;

WITH issue_rows AS (
    SELECT
        pb.id AS price_bar_id,
        stage.asset_id,
        stage.source,
        stage."interval",
        stage.bar_time,
        issue.issue_code,
        issue.field_name,
        CASE
            WHEN stage.quality_status = 'INVALID' THEN 'ERROR'
            ELSE 'WARNING'
        END::VARCHAR(20) AS severity,
        issue.observed_value,
        jsonb_build_object(
            'open', stage.open,
            'high', stage.high,
            'low', stage.low,
            'close', stage.close,
            'volume', stage.volume,
            'quality_status', stage.quality_status,
            'display_policy', stage.display_policy,
            'source_table', 'stock',
            'source_provider', 'KIS',
            'classification', 'LEGACY_STOCK_OHLC_VALIDATION'
        ) AS details
    FROM tmp_invalid_price_bar_stage stage
    JOIN price_bar pb
      ON pb.asset_id = stage.asset_id
     AND pb.source = stage.source
     AND pb."interval" = stage."interval"
     AND pb.bar_time = stage.bar_time
    CROSS JOIN LATERAL (
        VALUES
            (
                'HIGH_BELOW_PRICE'::VARCHAR(50),
                'high'::VARCHAR(30),
                stage.high,
                stage.high < GREATEST(stage.open, stage.close, stage.low)
            ),
            (
                'LOW_ABOVE_PRICE'::VARCHAR(50),
                'low'::VARCHAR(30),
                stage.low,
                stage.low > LEAST(stage.open, stage.close, stage.high)
            )
    ) AS issue(issue_code, field_name, observed_value, is_present)
    WHERE issue.is_present
)
INSERT INTO price_bar_quality_issue (
    price_bar_id,
    asset_id,
    source,
    "interval",
    bar_time,
    issue_code,
    field_name,
    severity,
    observed_value,
    issue_status,
    detected_at,
    last_seen_at,
    details
)
SELECT
    price_bar_id,
    asset_id,
    source,
    "interval",
    bar_time,
    issue_code,
    field_name,
    severity,
    observed_value,
    'OPEN',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    details
FROM issue_rows
ON CONFLICT (
    asset_id,
    source,
    "interval",
    bar_time,
    issue_code,
    field_name
)
DO UPDATE SET
    price_bar_id = EXCLUDED.price_bar_id,
    severity = EXCLUDED.severity,
    observed_value = EXCLUDED.observed_value,
    issue_status = 'OPEN',
    last_seen_at = EXCLUDED.last_seen_at,
    resolved_at = NULL,
    resolution_source = NULL,
    resolution_note = NULL,
    details = EXCLUDED.details;

ANALYZE price_bar;
ANALYZE price_bar_quality_issue;

