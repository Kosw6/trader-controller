-- Event dates are first split-adjusted trading dates, not announcement dates.
-- This is a reviewed subset, never a complete corporate-action history.
CREATE TABLE IF NOT EXISTS price_chart_corporate_action (
    id BIGSERIAL PRIMARY KEY,
    asset_id BIGINT NOT NULL REFERENCES asset(id),
    trading_date DATE NOT NULL,
    action_type TEXT NOT NULL CHECK (action_type IN ('SPLIT','REVERSE_SPLIT')),
    new_shares INTEGER NOT NULL CHECK (new_shares > 0),
    old_shares INTEGER NOT NULL CHECK (old_shares > 0),
    source_url TEXT NOT NULL CHECK (source_url LIKE 'https://%'),
    verified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((action_type='SPLIT' AND new_shares>old_shares)
        OR (action_type='REVERSE_SPLIT' AND new_shares<old_shares)),
    UNIQUE(asset_id,trading_date,action_type)
);

-- NVIDIA official 8-K: trading on split-adjusted basis begins June 10, 2024.
INSERT INTO price_chart_corporate_action
    (asset_id,trading_date,action_type,new_shares,old_shares,source_url)
SELECT DISTINCT asset_id,DATE '2024-06-10','SPLIT',10,1,
 'https://www.sec.gov/Archives/edgar/data/1045810/000104581024000144/nvda-20240607.htm'
FROM asset_provider_symbol WHERE provider='KIS' AND provider_symbol='NVDA' AND is_current
ON CONFLICT(asset_id,trading_date,action_type) DO NOTHING;
