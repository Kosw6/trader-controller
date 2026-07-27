CREATE TABLE IF NOT EXISTS admin_session (
    id BIGSERIAL PRIMARY KEY,
    session_token_hash CHAR(64) NOT NULL UNIQUE,
    actor VARCHAR(100) NOT NULL DEFAULT 'admin',
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_admin_session_active
ON admin_session (expires_at)
WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_admin_session_actor_recent
ON admin_session (actor, last_seen_at DESC);
