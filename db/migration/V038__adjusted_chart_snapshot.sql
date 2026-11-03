-- 일반 price_bar 및 주봉 CAGG를 덮지 않는 검토된 차트용 한정 스냅샷.
CREATE TABLE price_chart_snapshot (
 id BIGSERIAL PRIMARY KEY,
 asset_id BIGINT NOT NULL REFERENCES asset(id),
 source_object_id BIGINT NOT NULL REFERENCES source_object(id),
 source VARCHAR(30) NOT NULL CHECK (source='KIS'),
 currency VARCHAR(10) NOT NULL,
 basis TEXT NOT NULL CHECK (basis='KIS_PROVIDER_ADJUSTED'),
 volume_basis TEXT NOT NULL CHECK (volume_basis='PROVIDER_REPORTED'),
 coverage_start DATE NOT NULL,
 coverage_end DATE NOT NULL,
 collected_at TIMESTAMPTZ NOT NULL,
 published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 row_count INTEGER NOT NULL CHECK (row_count>0),
 content_hash CHAR(64) NOT NULL,
 CHECK (coverage_start<=coverage_end),
 UNIQUE (asset_id,source_object_id)
);
CREATE INDEX idx_price_chart_snapshot_asset ON price_chart_snapshot(asset_id,published_at DESC);
CREATE TABLE price_chart_snapshot_bar (
 snapshot_id BIGINT NOT NULL REFERENCES price_chart_snapshot(id),
 trade_date DATE NOT NULL,
 candidate_id BIGINT NOT NULL REFERENCES price_adjustment_candidate(id),
 open NUMERIC NOT NULL, high NUMERIC NOT NULL, low NUMERIC NOT NULL, close NUMERIC NOT NULL,
 volume NUMERIC,
 PRIMARY KEY(snapshot_id,trade_date),
 CHECK(low>0 AND low<=open AND low<=close AND high>=open AND high>=close),
 CHECK(volume IS NULL OR volume>=0)
);
COMMENT ON TABLE price_chart_snapshot IS '공급자 수정 OHLC의 한정 기간 검토본. 분할 전용/총수익률 승인 또는 AI 검색 승인이 아님';
