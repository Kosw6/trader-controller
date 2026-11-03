-- 검토본이 존재한다는 이유만으로 사용자 기본 차트를 전환하지 않는다.
-- 전체 계획 범위 검증 후 명시적으로 종목당 한 세대만 활성화한다.
ALTER TABLE price_chart_snapshot ADD COLUMN default_for_chart BOOLEAN NOT NULL DEFAULT false;
CREATE UNIQUE INDEX uq_price_chart_snapshot_default
ON price_chart_snapshot(asset_id) WHERE default_for_chart;
