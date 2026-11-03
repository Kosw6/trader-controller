package recoverycoord

import (
	"context"
	"time"
)

// HoldOwned records a non-retryable stage failure, never reviving a lost lease.
func (c *Coordinator) HoldOwned(ctx context.Context, l Lease) error {
	tx, w, err := c.lock(ctx, l.WorkKey)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	now, err := dbNow(ctx, tx)
	if err != nil {
		return err
	}
	if err = check(w, l, now); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE pipeline_recovery_work SET state='REVIEW',owner='',lease_until=NULL,fence=fence+1,progress_at=clock_timestamp() WHERE work_key=$1`, l.WorkKey)
	if err != nil {
		return err
	}
	return commit(ctx, tx)
}

type OperationStatus struct {
	Key               string     `json:"key"`
	Provider          string     `json:"provider"`
	Target            string     `json:"target"`
	Phase             string     `json:"phase"`
	State             string     `json:"state"`
	Requests          int64      `json:"requests"`
	Retries           int64      `json:"retries"`
	RemainingRequests int64      `json:"remainingRequests"`
	Claims            int64      `json:"claims"`
	Deadline          time.Time  `json:"deadline"`
	Heartbeat         *time.Time `json:"heartbeatAt"`
	Progress          time.Time  `json:"progressAt"`
	Outbox            *string    `json:"outboxStatus"`
	PendingRequests   int64      `json:"pendingRequests"`
}

type OperationSummary struct {
	Registered       int64     `json:"registered"`
	VerifiedSuccess  int64     `json:"verifiedSuccess"`
	VerifiedNoChange int64     `json:"verifiedNoChange"`
	HeldReview       int64     `json:"heldReview"`
	Unfinished       int64     `json:"unfinished"`
	CommitUnresolved int64     `json:"commitUnresolved"`
	CheckedAt        time.Time `json:"checkedAt"`
}

// One statement snapshot, not a count of iterator positions. Registered work is
// the denominator; it is NOT an assertion about unregistered legacy job items.
func (c *Coordinator) Summary(ctx context.Context) (OperationSummary, error) {
	var s OperationSummary
	err := c.pool.QueryRow(ctx, `WITH categorized AS (
 SELECT CASE
 WHEN w.state='COMPLETE' OR w.phase='COMPLETE' THEN CASE
   WHEN w.state<>'COMPLETE' OR w.phase<>'COMPLETE' OR w.owner<>'' OR w.lease_until IS NOT NULL OR c.published_result IS NULL
    OR EXISTS(SELECT 1 FROM pipeline_recovery_request q WHERE q.work_key=w.work_key AND q.outcome='PENDING')
    OR NOT EXISTS(SELECT 1 FROM pipeline_recovery_receipt r WHERE r.work_key=w.work_key AND r.from_phase='PUBLISHED' AND r.to_phase='COMPLETE' AND r.content_hash=c.content_hash)
    OR (SELECT count(*) FROM pipeline_recovery_receipt r WHERE r.work_key=w.work_key)<>6
    OR EXISTS(SELECT 1 FROM (VALUES ('FETCH_RAW','RAW_READY'),('RAW_READY','ETL_READY'),('ETL_READY','CANDIDATE'),('CANDIDATE','VERIFIED'),('VERIFIED','PUBLISHED'),('PUBLISHED','COMPLETE')) e(a,b)
      WHERE NOT EXISTS(SELECT 1 FROM pipeline_recovery_receipt r WHERE r.work_key=w.work_key AND r.from_phase=e.a AND r.to_phase=e.b))
    OR EXISTS(SELECT 1 FROM pipeline_recovery_receipt r WHERE r.work_key=w.work_key AND r.from_phase NOT IN ('FETCH_RAW','RAW_READY') AND r.content_hash IS DISTINCT FROM c.content_hash)
    OR (SELECT count(DISTINCT r.content_hash) FROM pipeline_recovery_receipt r WHERE r.work_key=w.work_key AND r.from_phase IN ('FETCH_RAW','RAW_READY'))<>1 THEN 'unresolved'
   WHEN c.published_result->>'needsReview'='true' THEN 'review'
   WHEN c.published_result->>'inserted'='0' THEN 'no_change' ELSE 'success' END
 WHEN EXISTS(SELECT 1 FROM pipeline_recovery_request q WHERE q.work_key=w.work_key AND q.outcome='PENDING') THEN 'unresolved'
 WHEN w.state='REVIEW' THEN 'review'
 ELSE 'unfinished' END AS bucket
 FROM pipeline_recovery_work w LEFT JOIN pipeline_recovery_candidate c USING(work_key))
 SELECT count(*),count(*) FILTER(WHERE bucket='success'),count(*) FILTER(WHERE bucket='no_change'),
 count(*) FILTER(WHERE bucket='review'),count(*) FILTER(WHERE bucket='unfinished'),count(*) FILTER(WHERE bucket='unresolved'),clock_timestamp() FROM categorized`).Scan(
		&s.Registered, &s.VerifiedSuccess, &s.VerifiedNoChange, &s.HeldReview, &s.Unfinished, &s.CommitUnresolved, &s.CheckedAt)
	return s, err
}

// Read-only bounded page. Heartbeat/progress timestamps are not proof of liveness.
func (c *Coordinator) StatusPage(ctx context.Context, after string) ([]OperationStatus, error) {
	if after != "" && !digest.MatchString(after) {
		return nil, ErrInvalid
	}
	rows, err := c.pool.Query(ctx, `SELECT w.work_key,w.identity->>'provider',w.identity->>'target',w.phase,w.state,
 w.requests,w.retries,GREATEST(0,(w.policy->>'maxRequests')::bigint-w.requests),w.claims,w.deadline,w.heartbeat_at,w.progress_at,o.status,
 (SELECT count(*) FROM pipeline_recovery_request r WHERE r.work_key=w.work_key AND r.outcome='PENDING')
 FROM pipeline_recovery_work w LEFT JOIN pipeline_recovery_raw_intent i USING(work_key)
 LEFT JOIN pipeline_outbox o ON o.id=i.outbox_id WHERE w.work_key>$1 ORDER BY w.work_key LIMIT 100`, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []OperationStatus{}
	for rows.Next() {
		var s OperationStatus
		if err = rows.Scan(&s.Key, &s.Provider, &s.Target, &s.Phase, &s.State, &s.Requests, &s.Retries, &s.RemainingRequests, &s.Claims, &s.Deadline, &s.Heartbeat, &s.Progress, &s.Outbox, &s.PendingRequests); err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}
