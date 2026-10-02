ALTER TABLE ai.initial_progress ADD COLUMN terminal_proof text;
CREATE TABLE ai.initial_notification (
 run_id uuid PRIMARY KEY,
 status varchar(16) NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING','CONFIRMED','RECORDED_LATE','FAILED')),
 attempts integer NOT NULL DEFAULT 0,
 next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 lease_token uuid,lease_until timestamptz,last_error varchar(64),acknowledged_at timestamptz
);
