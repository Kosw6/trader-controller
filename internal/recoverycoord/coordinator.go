package recoverycoord

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Coordinator struct {
	pool     *pgxpool.Pool
	rawTopic string
}

const DefaultRawTopic = "trader.raw.coordinated.ready"

func New(pool *pgxpool.Pool) *Coordinator { return &Coordinator{pool: pool, rawTopic: DefaultRawTopic} }

// NewWithRawTopic keeps isolated deployments on their own outbox topic. It never
// rewrites or relaxes a database topic fence; deployment must authorize both ends.
func NewWithRawTopic(pool *pgxpool.Pool, topic string) (*Coordinator, error) {
	if topic != DefaultRawTopic && topic != DefaultRawTopic+"649" {
		return nil, ErrInvalid
	}
	return &Coordinator{pool: pool, rawTopic: topic}, nil
}

const columns = `work_key,identity,policy,scope_key,phase,state,fence,owner,lease_until,deadline,claims,requests,retries,heartbeat_at,progress_at,clock_timestamp()`

func scan(row pgx.Row) (Work, error) {
	var w Work
	var identity, policy []byte
	err := row.Scan(&w.Key, &identity, &policy, &w.Scope, &w.Phase, &w.State, &w.Fence, &w.Owner, &w.LeaseUntil, &w.Deadline, &w.Claims, &w.Requests, &w.Retries, &w.HeartbeatAt, &w.ProgressAt, &w.ServerNow)
	if err != nil {
		return w, err
	}
	if err = json.Unmarshal(identity, &w.Identity); err != nil {
		return w, err
	}
	err = json.Unmarshal(policy, &w.Policy)
	return w, err
}
func commit(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		return ErrUncertain
	}
	return nil
}
func (c *Coordinator) Read(ctx context.Context, key string) (Work, error) {
	if !digest.MatchString(key) {
		return Work{}, ErrInvalid
	}
	return scan(c.pool.QueryRow(ctx, `SELECT `+columns+` FROM pipeline_recovery_work WHERE work_key=$1`, key))
}
func (c *Coordinator) Register(ctx context.Context, i Identity, p Policy) (Work, error) {
	if i.validate() != nil || p.validate() != nil {
		return Work{}, ErrInvalid
	}
	tx, err := c.begin(ctx)
	if err != nil {
		return Work{}, err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO pipeline_recovery_quota(scope_key,interval_ms) VALUES($1,$2) ON CONFLICT DO NOTHING`, i.Scope(), p.IntervalMS)
	if err != nil {
		return Work{}, err
	}
	var interval int64
	err = tx.QueryRow(ctx, `SELECT interval_ms FROM pipeline_recovery_quota WHERE scope_key=$1`, i.Scope()).Scan(&interval)
	if err != nil {
		return Work{}, err
	}
	if interval != p.IntervalMS {
		return Work{}, ErrConflict
	}
	ib, _ := json.Marshal(i)
	pb, _ := json.Marshal(p)
	_, err = tx.Exec(ctx, `INSERT INTO pipeline_recovery_work(work_key,identity,policy,scope_key,deadline) VALUES($1,$2,$3,$4,clock_timestamp()+$5*interval '1 millisecond') ON CONFLICT DO NOTHING`, i.Key(), ib, pb, i.Scope(), p.MaxElapsedMS)
	if err != nil {
		return Work{}, err
	}
	w, err := scan(tx.QueryRow(ctx, `SELECT `+columns+` FROM pipeline_recovery_work WHERE work_key=$1`, i.Key()))
	if err != nil {
		return w, err
	}
	if w.Identity != i || w.Policy != p {
		return Work{}, ErrConflict
	}
	return w, commit(ctx, tx)
}
func (c *Coordinator) begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	// Bounded lock wait; never retry a failed transaction automatically.
	_, err = tx.Exec(ctx, `SET LOCAL lock_timeout='2s'`)
	if err != nil {
		tx.Rollback(ctx)
		return nil, err
	}
	_, err = tx.Exec(ctx, `SET LOCAL statement_timeout='10s'`)
	if err != nil {
		tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}
func (c *Coordinator) lock(ctx context.Context, key string) (pgx.Tx, Work, error) {
	if !digest.MatchString(key) {
		return nil, Work{}, ErrInvalid
	}
	tx, err := c.begin(ctx)
	if err != nil {
		return nil, Work{}, err
	}
	w, err := scan(tx.QueryRow(ctx, `SELECT `+columns+` FROM pipeline_recovery_work WHERE work_key=$1 FOR UPDATE`, key))
	if err != nil {
		tx.Rollback(ctx)
		return nil, w, err
	}
	return tx, w, nil
}
func dbNow(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now)
	return now, err
}
func check(w Work, l Lease, now time.Time) error {
	if !validLease(l) || w.State != "LEASED" || w.Owner != l.Owner || w.Fence != l.Fence || w.LeaseUntil == nil || !now.Before(*w.LeaseUntil) {
		return ErrLease
	}
	if !now.Before(w.Deadline) {
		return ErrBudget
	}
	return nil
}
func (c *Coordinator) Claim(ctx context.Context, key, owner string) (Work, error) {
	if !identifier.MatchString(owner) {
		return Work{}, ErrInvalid
	}
	tx, w, err := c.lock(ctx, key)
	if err != nil {
		return w, err
	}
	defer tx.Rollback(ctx)
	now, err := dbNow(ctx, tx)
	if err != nil {
		return w, err
	}
	if w.State == "PAUSED" || w.State == "REVIEW" || w.State == "COMPLETE" {
		return w, ErrReview
	}
	if w.LeaseUntil != nil && now.Before(*w.LeaseUntil) {
		return w, ErrLease
	}
	if !now.Before(w.Deadline) || w.Claims >= w.Policy.MaxClaims {
		return w, ErrBudget
	}
	var pending bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pipeline_recovery_request WHERE work_key=$1 AND outcome='PENDING')`, key).Scan(&pending)
	if err != nil {
		return w, err
	}
	if pending {
		return w, ErrReview
	} // A dead process is not proof that its request never happened.
	w, err = scan(tx.QueryRow(ctx, `UPDATE pipeline_recovery_work SET state='LEASED',owner=$2,fence=fence+1,claims=claims+1,heartbeat_at=clock_timestamp(),lease_until=LEAST(deadline,clock_timestamp()+$3*interval '1 millisecond') WHERE work_key=$1 RETURNING `+columns, key, owner, w.Policy.LeaseMS))
	if err != nil {
		return w, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO pipeline_recovery_claim(work_key,fence,owner) VALUES($1,$2,$3)`, key, w.Fence, owner)
	if err != nil {
		return w, err
	}
	return w, commit(ctx, tx)
}
func (c *Coordinator) Renew(ctx context.Context, l Lease) (Work, error) {
	tx, w, err := c.lock(ctx, l.WorkKey)
	if err != nil {
		return w, err
	}
	defer tx.Rollback(ctx)
	now, err := dbNow(ctx, tx)
	if err != nil {
		return w, err
	}
	if err = check(w, l, now); err != nil {
		return w, err
	}
	w, err = scan(tx.QueryRow(ctx, `UPDATE pipeline_recovery_work SET heartbeat_at=clock_timestamp(),lease_until=LEAST(deadline,clock_timestamp()+$2*interval '1 millisecond') WHERE work_key=$1 AND lease_until>clock_timestamp() AND deadline>clock_timestamp() RETURNING `+columns, l.WorkKey, w.Policy.LeaseMS))
	if errors.Is(err, pgx.ErrNoRows) {
		return w, ErrLease
	}
	if err != nil {
		return w, err
	}
	return w, commit(ctx, tx)
}

// Reserve must commit BEFORE exactly one HTTP send. Repeated request IDs never
// grant another send, even if the first reservation's response was lost.
func (c *Coordinator) Reserve(ctx context.Context, l Lease, requestID, operation string) error {
	if !identifier.MatchString(requestID) || !identifier.MatchString(operation) {
		return ErrInvalid
	}
	tx, w, err := c.lock(ctx, l.WorkKey)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var next time.Time
	err = tx.QueryRow(ctx, `SELECT next_at FROM pipeline_recovery_quota WHERE scope_key=$1 FOR UPDATE`, w.Scope).Scan(&next)
	if err != nil {
		return err
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return err
	}
	if err = check(w, l, now); err != nil {
		return err
	}
	if w.Phase != "FETCH_RAW" {
		return ErrReview
	}
	var exists bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pipeline_recovery_request WHERE work_key=$1 AND request_id=$2)`, w.Key, requestID).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return ErrReserved
	}
	if now.Before(next) {
		return ErrCooling
	}
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pipeline_recovery_request WHERE scope_key=$1 AND outcome='PENDING')`, w.Scope).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return ErrCooling
	}
	var outcome string
	err = tx.QueryRow(ctx, `SELECT outcome FROM pipeline_recovery_request WHERE work_key=$1 AND operation_key=$2 ORDER BY reserved_at DESC,request_id DESC LIMIT 1`, w.Key, operation).Scan(&outcome)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	retry := int64(0)
	if err == nil {
		if outcome != "TRANSIENT_TRANSPORT" && outcome != "RATE_LIMIT" {
			return ErrReview
		}
		retry = 1
	}
	if w.Requests >= w.Policy.MaxRequests || (retry == 1 && w.Retries >= w.Policy.MaxRetries) {
		return ErrBudget
	}
	_, err = tx.Exec(ctx, `INSERT INTO pipeline_recovery_request(work_key,request_id,operation_key,scope_key,fence) VALUES($1,$2,$3,$4,$5)`, w.Key, requestID, operation, w.Scope, l.Fence)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE pipeline_recovery_work SET requests=requests+1,retries=retries+$2 WHERE work_key=$1`, w.Key, retry)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE pipeline_recovery_quota SET next_at=clock_timestamp()+interval_ms*interval '1 millisecond' WHERE scope_key=$1`, w.Scope)
	if err != nil {
		return err
	}
	return commit(ctx, tx)
}

// Report accepts only allowlisted classifications, never provider error prose.
func (c *Coordinator) Report(ctx context.Context, l Lease, id, outcome string, retryAfterMS int64) error {
	allowed := map[string]bool{"HTTP_OK": true, "TRANSIENT_TRANSPORT": true, "RATE_LIMIT": true, "AUTH_BLOCKED": true, "EMPTY_UNRESOLVED": true, "SOURCE_NOT_FOUND": true, "QUALITY_OR_SEMANTIC_CONFLICT": true, "COMMIT_UNCERTAIN": true, "IMPLEMENTATION_ERROR": true, "RESOURCE_BLOCKED": true, "USER_PAUSED": true, "UNCLASSIFIED": true}
	if !identifier.MatchString(id) || !allowed[outcome] || retryAfterMS < 0 || retryAfterMS > 604800000 {
		return ErrInvalid
	}
	tx, w, err := c.lock(ctx, l.WorkKey)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var unused time.Time
	err = tx.QueryRow(ctx, `SELECT next_at FROM pipeline_recovery_quota WHERE scope_key=$1 FOR UPDATE`, w.Scope).Scan(&unused)
	if err != nil {
		return err
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return err
	}
	if err = check(w, l, now); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE pipeline_recovery_request SET outcome=$4,reported_at=clock_timestamp() WHERE work_key=$1 AND request_id=$2 AND fence=$3 AND outcome='PENDING'`, w.Key, id, l.Fence, outcome)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrReserved
	}
	delay := retryAfterMS
	if outcome == "TRANSIENT_TRANSPORT" || outcome == "RATE_LIMIT" {
		backoff := w.Policy.BaseDelayMS
		for n := int64(0); n < w.Retries && backoff < w.Policy.MaxDelayMS; n++ {
			backoff = min(backoff*2, w.Policy.MaxDelayMS)
		}
		backoff = int64(math.Ceil(float64(backoff) * (0.5 + rand.Float64()*0.5)))
		delay = max(delay, backoff)
		if outcome == "RATE_LIMIT" {
			delay = max(delay, w.Policy.RateLimitMS)
		}
	}
	if delay > 0 {
		_, err = tx.Exec(ctx, `UPDATE pipeline_recovery_quota SET next_at=GREATEST(next_at,clock_timestamp()+$2*interval '1 millisecond') WHERE scope_key=$1`, w.Scope, delay)
		if err != nil {
			return err
		}
	}
	if outcome != "HTTP_OK" && outcome != "TRANSIENT_TRANSPORT" && outcome != "RATE_LIMIT" {
		state := "REVIEW"
		if outcome == "USER_PAUSED" {
			state = "PAUSED"
		}
		_, err = tx.Exec(ctx, `UPDATE pipeline_recovery_work SET state=$2,owner='',lease_until=NULL,fence=fence+1 WHERE work_key=$1`, w.Key, state)
		if err != nil {
			return err
		}
	}
	return commit(ctx, tx)
}
func (c *Coordinator) Pause(ctx context.Context, key string) error {
	tx, w, err := c.lock(ctx, key)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if w.State == "COMPLETE" || w.State == "REVIEW" {
		return ErrReview
	}
	_, err = tx.Exec(ctx, `UPDATE pipeline_recovery_work SET state='PAUSED',owner='',lease_until=NULL,fence=fence+1 WHERE work_key=$1`, key)
	if err != nil {
		return err
	}
	return commit(ctx, tx)
}

// PauseOwned propagates cancellation observed by a worker, never by a stale owner.
func (c *Coordinator) PauseOwned(ctx context.Context, l Lease) error {
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
	_, err = tx.Exec(ctx, `UPDATE pipeline_recovery_work SET state='PAUSED',owner='',lease_until=NULL,fence=fence+1 WHERE work_key=$1`, w.Key)
	if err != nil {
		return err
	}
	return commit(ctx, tx)
}

// Resume is an explicit operator action; it never resets a budget/deadline or
// resolves an unknown request. REVIEW needs independent reconciliation first.
func (c *Coordinator) Resume(ctx context.Context, key string) error {
	tx, w, err := c.lock(ctx, key)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if w.State != "PAUSED" {
		return ErrReview
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return err
	}
	if !now.Before(w.Deadline) || w.Claims >= w.Policy.MaxClaims || (w.Phase == "FETCH_RAW" && w.Requests >= w.Policy.MaxRequests) {
		return ErrBudget
	}
	var pending bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pipeline_recovery_request WHERE work_key=$1 AND outcome='PENDING')`, key).Scan(&pending)
	if err != nil {
		return err
	}
	if pending {
		return ErrReview
	}
	_, err = tx.Exec(ctx, `UPDATE pipeline_recovery_work SET state='READY' WHERE work_key=$1`, key)
	if err != nil {
		return err
	}
	return commit(ctx, tx)
}

// SQL exposes only the supplied transaction's statements, not Commit/Rollback.
// Callback code must not make network requests or use another DB connection.
type SQL interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}
type fencedSQL struct{ tx pgx.Tx }

func (s fencedSQL) Exec(ctx context.Context, q string, args ...any) (pgconn.CommandTag, error) {
	return s.tx.Exec(ctx, q, args...)
}
func (s fencedSQL) QueryRow(ctx context.Context, q string, args ...any) pgx.Row {
	return s.tx.QueryRow(ctx, q, args...)
}

// Apply writes data and its result receipt atomically under a live fencing lock.
// A lost commit response MUST be reconciled via ReadReceipt, never blind replay.
func (c *Coordinator) Apply(ctx context.Context, l Lease, result Receipt, write func(context.Context, SQL) error) error {
	if !identifier.MatchString(result.ID) || !digest.MatchString(result.Hash) || !nextPhase(result.From, result.To) || write == nil {
		return ErrInvalid
	}
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
	var exists bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pipeline_recovery_receipt WHERE work_key=$1 AND receipt_id=$2)`, w.Key, result.ID).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return ErrReceipt
	}
	if w.Phase != result.From {
		return ErrReview
	}
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pipeline_recovery_request WHERE work_key=$1 AND outcome='PENDING')`, w.Key).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return ErrReview
	}
	if err = write(ctx, fencedSQL{tx}); err != nil {
		return err
	}
	// Clock is checked AFTER the callback too: an expired owner's writes roll back.
	now, err = dbNow(ctx, tx)
	if err != nil {
		return err
	}
	if err = check(w, l, now); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO pipeline_recovery_receipt(work_key,receipt_id,from_phase,to_phase,content_hash,fence) VALUES($1,$2,$3,$4,$5,$6)`, w.Key, result.ID, result.From, result.To, result.Hash, l.Fence)
	if err != nil {
		return err
	}
	state := "READY"
	if result.To == "COMPLETE" {
		state = "COMPLETE"
	}
	tag, err := tx.Exec(ctx, `UPDATE pipeline_recovery_work SET phase=$2,state=$3,owner='',lease_until=NULL,progress_at=clock_timestamp() WHERE work_key=$1 AND fence=$4 AND state='LEASED' AND lease_until>clock_timestamp() AND deadline>clock_timestamp()`, w.Key, result.To, state, l.Fence)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLease
	}
	return commit(ctx, tx)
}
func (c *Coordinator) ReadReceipt(ctx context.Context, key, id string) (Receipt, error) {
	var r Receipt
	if !digest.MatchString(key) || !identifier.MatchString(id) {
		return r, ErrInvalid
	}
	err := c.pool.QueryRow(ctx, `SELECT receipt_id,from_phase,to_phase,content_hash,fence FROM pipeline_recovery_receipt WHERE work_key=$1 AND receipt_id=$2`, key, id).Scan(&r.ID, &r.From, &r.To, &r.Hash, &r.Fence)
	return r, err
}

type RequestReceipt struct {
	ID        string `json:"id"`
	Operation string `json:"operation"`
	Outcome   string `json:"outcome"`
	Fence     int64  `json:"fence"`
}

func (c *Coordinator) ReadRequest(ctx context.Context, key, id string) (RequestReceipt, error) {
	var r RequestReceipt
	if !digest.MatchString(key) || !identifier.MatchString(id) {
		return r, ErrInvalid
	}
	err := c.pool.QueryRow(ctx, `SELECT request_id,operation_key,outcome,fence FROM pipeline_recovery_request WHERE work_key=$1 AND request_id=$2`, key, id).Scan(&r.ID, &r.Operation, &r.Outcome, &r.Fence)
	return r, err
}
