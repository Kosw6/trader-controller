-- Apply in the application schema before deploying the matching backend.
-- Retain APPROVED/REJECTED rows and issue a fresh request ID on reapplication.
BEGIN;
SET lock_timeout = '3s';
LOCK TABLE join_request IN SHARE ROW EXCLUSIVE MODE;
CREATE UNIQUE INDEX uq_join_request_pending
    ON join_request(team_id, requester_id) WHERE status = 'PENDING';
ALTER TABLE join_request DROP CONSTRAINT uq_join_request_team_requester;
RESET lock_timeout;
COMMIT;
