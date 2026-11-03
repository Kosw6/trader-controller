-- Request IDs and receipts only. No prompts, credentials or conversation history.
SET lock_timeout = '3s';
CREATE TABLE canvas_ai_run (
 id uuid PRIMARY KEY,
 owner_id bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 team_id bigint NOT NULL REFERENCES team(id) ON DELETE CASCADE,
 page_id bigint NOT NULL REFERENCES page(id) ON DELETE CASCADE,
 goal_hash varchar(64) NOT NULL CHECK (goal_hash ~ '^[0-9a-f]{64}$'),
 base_fingerprint varchar(64) NOT NULL,
 status varchar(16) NOT NULL CHECK (status IN ('CLAIMED','APPLIED','UNDONE','CANCELLED','FAILED','EXPIRED')),
 receipt jsonb,
 after_fingerprint varchar(64),
 created_at timestamptz NOT NULL DEFAULT now(),
 expires_at timestamptz NOT NULL DEFAULT (now() + interval '120 seconds'),
 completed_at timestamptz
);
CREATE UNIQUE INDEX canvas_ai_run_one_active ON canvas_ai_run(page_id) WHERE status='CLAIMED';
CREATE INDEX canvas_ai_run_owner ON canvas_ai_run(owner_id,created_at DESC);
RESET lock_timeout;
