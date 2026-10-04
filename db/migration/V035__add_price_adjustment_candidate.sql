-- 공급자 조정 가격은 검증 전 일반 OHLC / AI 승인 가격으로 승격하지 않는다.
CREATE TABLE price_adjustment_candidate (
    id BIGSERIAL PRIMARY KEY,
    source_object_id BIGINT NOT NULL REFERENCES source_object(id),
    asset_id BIGINT NOT NULL REFERENCES asset(id),
    source VARCHAR(30) NOT NULL,
    "interval" VARCHAR(10) NOT NULL,
    trade_date DATE NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume NUMERIC,
    currency VARCHAR(10) NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    content_hash CHAR(64) NOT NULL,
    basis TEXT NOT NULL DEFAULT 'PROVIDER_ADJUSTED_UNREVIEWED'
        CHECK (basis = 'PROVIDER_ADJUSTED_UNREVIEWED'),
    UNIQUE (source_object_id, asset_id, source, "interval", trade_date),
    CHECK (low > 0 AND low <= open AND low <= close AND high >= open AND high >= close),
    CHECK (volume IS NULL OR volume >= 0)
);
CREATE INDEX idx_price_adjustment_candidate_review
    ON price_adjustment_candidate(asset_id, source, trade_date, collected_at);
COMMENT ON TABLE price_adjustment_candidate IS
    '검토 전 공급자 수정주가 원문별 후보. 분할 전용 조정 여부 및 승인과 별개';
COMMENT ON COLUMN price_adjustment_candidate.trade_date IS
    '공급자가 반환한 거래일. UTC 자정 시각을 시장 시간대로 변환하지 않는다';
