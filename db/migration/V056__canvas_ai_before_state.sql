-- Private reversible edit snapshots. Never expose them through public receipts.
-- Parent user/team/page deletion already cascades through canvas_ai_run.
SET lock_timeout = '3s';
ALTER TABLE canvas_ai_run ADD COLUMN before_state jsonb;
ALTER TABLE canvas_ai_run ADD CONSTRAINT canvas_ai_before_state_array
 CHECK (before_state IS NULL OR (jsonb_typeof(before_state)='array'
   AND jsonb_array_length(before_state)<=6));
RESET lock_timeout;
