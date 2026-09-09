-- Fence stale realtime outbox workers after a publishing lease is recovered.
-- claim_token identifies one claim attempt; claimed_by is operational metadata.
ALTER TABLE realtime_outbox
    ADD COLUMN IF NOT EXISTS claim_token VARCHAR(36),
    ADD COLUMN IF NOT EXISTS claimed_by VARCHAR(100),
    ADD COLUMN IF NOT EXISTS lease_until TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_realtime_outbox_status_lease
    ON realtime_outbox (status, lease_until);

COMMENT ON COLUMN realtime_outbox.claim_token IS
    'One-time fencing token for the current PUBLISHING claim';

COMMENT ON COLUMN realtime_outbox.claimed_by IS
    'Application instance that owns the current claim';

COMMENT ON COLUMN realtime_outbox.lease_until IS
    'Claim expiry used by stale PUBLISHING recovery';
