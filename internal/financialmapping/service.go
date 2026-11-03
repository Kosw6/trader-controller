// Package financialmapping controls only stored financial-tag mapping, never collection.
package financialmapping

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kosw006/trader-controller/internal/store"
	"github.com/robfig/cron/v3"
)

type Params struct {
	Mode         string  `json:"mode,omitempty"`
	CompanyIDs   []int64 `json:"companyIds"`
	AllCompanies bool    `json:"allCompanies"`
	StartDate    string  `json:"startDate"`
	EndDate      string  `json:"endDate"`
	AnnualOnly   bool    `json:"annualOnly"`
}
type Request struct {
	Params
	RequestKey string `json:"requestKey"`
}
type ScheduleRequest struct {
	Name       string `json:"name"`
	Cron       string `json:"cronExpression"`
	Timezone   string `json:"timezone"`
	Parameters Params `json:"parameters"`
	Enabled    bool   `json:"enabled"`
}
type Schedule struct {
	ScheduleRequest
	ID         int64      `json:"id"`
	NextRun    *time.Time `json:"nextRunAt"`
	LastRun    *time.Time `json:"lastRunAt"`
	LastStatus *string    `json:"lastStatus"`
}
type Run struct {
	ScheduleID      *int64    `json:"scheduleId,omitempty"`
	ID              int64     `json:"id"`
	Parameters      Params    `json:"parameters"`
	RequestedBy     string    `json:"requestedBy"`
	CreatedAt       time.Time `json:"createdAt"`
	Companies       int       `json:"companies"`
	Completed       int       `json:"completed"`
	Pending         int       `json:"pending"`
	ReviewCompanies int       `json:"reviewCompanies"`
	Inserted        int64     `json:"insertedMappings"`
	Existing        int64     `json:"existingMappings"`
	Held            int64     `json:"heldMappings"`
	Unverified      int       `json:"unverified"`
}
type Held struct {
	ID        int64  `json:"id"`
	CompanyID int64  `json:"companyId"`
	MetricID  int64  `json:"metricId"`
	End       string `json:"periodEnd"`
	Reason    string `json:"reason"`
}
type State struct {
	Runs             []Run      `json:"runs"`
	Schedules        []Schedule `json:"schedules"`
	Held             []Held     `json:"held"`
	PendingCompanies int        `json:"pendingCompanies"`
	CheckedAt        time.Time  `json:"checkedAt"`
}
type Service struct {
	pool                  *pgxpool.Pool
	now                   func() time.Time
	reconciliationEnabled bool
	rawRepairEnabled      bool
}

func New(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool, now: time.Now, reconciliationEnabled: os.Getenv("FINANCIAL_RECONCILIATION_ENABLED") == "true", rawRepairEnabled: os.Getenv("FINANCIAL_RAW_VERIFIED_REPAIR_ENABLED") == "true"}
}
func (s *Service) allowMode(p Params) error {
	if p.Mode == "reviewedReconciliation" && !s.reconciliationEnabled {
		return bad("검토된 조정표 워커를 설치하고 모드를 활성화한 후 실행하세요")
	}
	if p.Mode == "rawVerifiedRepair" && !s.rawRepairEnabled {
		return bad("원문 검증 워커와 증거를 준비하고 복구 모드를 활성화한 후 실행하세요")
	}
	return nil
}
func bad(message string) error { return fmt.Errorf("%w: %s", store.ErrValidation, message) }

var keyPattern = regexp.MustCompile(`^[A-Za-z0-9:_-]{8,100}$`)

func queueReason(p Params) string {
	if p.Mode == "rawVerifiedRepair" {
		return "SEC_RAW_REPAIR674"
	}
	if p.Mode == "reviewedReconciliation" {
		return "REVIEWED_RECONCILIATION677"
	}
	return "ADMIN_MAPPING"
}

func Validate(p *Params, now time.Time) error {
	if p.Mode != "" && p.Mode != "reviewedReconciliation" && p.Mode != "rawVerifiedRepair" {
		return bad("지원하지 않는 매핑 모드입니다")
	}
	if p.Mode == "rawVerifiedRepair" && (p.AllCompanies || len(p.CompanyIDs) > 50 || p.EndDate == "") {
		return bad("원문 검증 복구는 기업 ID 최대 50개와 종료일을 지정하세요")
	}
	if p.Mode == "reviewedReconciliation" && (!p.AnnualOnly || p.AllCompanies || len(p.CompanyIDs) > 50) {
		return bad("검토된 조정표 실행은 연간 전용이며 기업 ID 최대 50개를 지정하세요")
	}
	if p.AllCompanies == (len(p.CompanyIDs) > 0) {
		return bad("전체 기업 또는 기업 ID 목록 중 하나를 지정하세요")
	}
	if len(p.CompanyIDs) > 5000 {
		return bad("기업은 최대 5000개까지 지정하세요")
	}
	seen := map[int64]bool{}
	for _, id := range p.CompanyIDs {
		if id <= 0 || seen[id] {
			return bad("기업 ID는 양수이고 중복되지 않아야 합니다")
		}
		seen[id] = true
	}
	sort.Slice(p.CompanyIDs, func(i, j int) bool { return p.CompanyIDs[i] < p.CompanyIDs[j] })
	start, e := time.Parse("2006-01-02", p.StartDate)
	if e != nil || start.Year() < 1900 {
		return bad("시작일 형식은 YYYY-MM-DD이며 1900년 이후여야 합니다")
	}
	end := now.UTC()
	if p.EndDate != "" {
		end, e = time.Parse("2006-01-02", p.EndDate)
		if e != nil {
			return bad("종료일 형식은 YYYY-MM-DD입니다")
		}
	}
	if start.After(end) || end.Format("2006-01-02") > now.UTC().Format("2006-01-02") {
		return bad("종료일은 시작일 이후이면서 오늘까지여야 합니다")
	}
	return nil
}
func next(req ScheduleRequest, now time.Time) (time.Time, error) {
	if strings.TrimSpace(req.Name) == "" || len(req.Name) > 100 {
		return time.Time{}, bad("예약 이름은 1~100자입니다")
	}
	loc, e := time.LoadLocation(req.Timezone)
	if e != nil {
		return time.Time{}, bad("지원하지 않는 시간대입니다")
	}
	if len(strings.Fields(req.Cron)) != 5 {
		return time.Time{}, bad("5자리 cron 표현식을 입력하세요")
	}
	c, e := cron.ParseStandard(req.Cron)
	if e != nil {
		return time.Time{}, bad("5자리 cron 표현식을 확인하세요")
	}
	n := c.Next(now.In(loc))
	if n.IsZero() || c.Next(n).Sub(n) < 15*time.Minute {
		return time.Time{}, bad("예약 간격은 15분 이상이어야 합니다")
	}
	return n, nil
}
func mapped(e error) error {
	var p *pgconn.PgError
	if errors.As(e, &p) && (p.Code == "55P03" || p.Code == "23505") {
		return store.ErrConflict
	}
	return e
}
func (s *Service) Start(ctx context.Context, req Request, actor string) (Run, error) {
	if e := s.allowMode(req.Params); e != nil {
		return Run{}, e
	}
	if !keyPattern.MatchString(req.RequestKey) {
		return Run{}, bad("유효한 requestKey가 필요합니다")
	}
	if e := Validate(&req.Params, s.now()); e != nil {
		return Run{}, e
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return Run{}, e
	}
	defer tx.Rollback(ctx)
	id, e := s.enqueue(ctx, tx, req, actor, nil)
	if e != nil {
		return Run{}, mapped(e)
	}
	if e = tx.Commit(ctx); e != nil {
		return Run{}, e
	}
	return s.run(ctx, id)
}
func (s *Service) enqueue(ctx context.Context, tx pgx.Tx, req Request, actor string, scheduleID *int64) (int64, error) {
	if e := s.allowMode(req.Params); e != nil {
		return 0, e
	}
	if _, e := tx.Exec(ctx, "SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='8s'"); e != nil {
		return 0, e
	}
	var locked bool
	if e := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock(668001)").Scan(&locked); e != nil {
		return 0, e
	}
	if !locked {
		return 0, store.ErrConflict
	}
	body, _ := json.Marshal(req.Params)
	var prior int64
	var same bool
	e := tx.QueryRow(ctx, "SELECT id,parameters=$2::jsonb FROM finance_mapping_ops668.run WHERE request_key=$1", req.RequestKey, body).Scan(&prior, &same)
	if e == nil {
		if !same {
			return 0, store.ErrConflict
		}
		return prior, nil
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return 0, e
	}
	var pending bool
	if e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM finance_mapping667.queue WHERE status='PENDING')").Scan(&pending); e != nil {
		return 0, e
	}
	if pending {
		return 0, store.ErrConflict
	}
	rows, e := tx.Query(ctx, "SELECT id FROM public.company WHERE $1 OR id=ANY($2::bigint[]) ORDER BY id LIMIT 5001", req.AllCompanies, req.CompanyIDs)
	if e != nil {
		return 0, e
	}
	ids := []int64{}
	for rows.Next() {
		var id int64
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return 0, e
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return 0, e
	}
	if len(ids) == 0 || len(ids) > 5000 || !req.AllCompanies && len(ids) != len(req.CompanyIDs) {
		return 0, bad("존재하는 기업 ID를 지정하세요. 한 번에 최대 5000개입니다")
	}
	// Lock selected queue rows without waiting behind a running company transaction.
	rows, e = tx.Query(ctx, "SELECT company_id FROM finance_mapping667.queue WHERE company_id=ANY($1::bigint[]) FOR UPDATE NOWAIT", ids)
	if e != nil {
		return 0, e
	}
	for rows.Next() {
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return 0, e
	}
	var id int64
	e = tx.QueryRow(ctx, "INSERT INTO finance_mapping_ops668.run(request_key,parameters,requested_by,schedule_id) VALUES($1,$2,$3,$4) RETURNING id", req.RequestKey, body, actor, scheduleID).Scan(&id)
	if e != nil {
		return 0, e
	}
	end := req.EndDate
	if end == "" {
		end = s.now().UTC().Format("2006-01-02")
	}
	_, e = tx.Exec(ctx, `WITH queued AS (
 INSERT INTO finance_mapping667.queue(company_id,status,reason,start_date,end_date,annual_only)
 SELECT unnest($1::bigint[]),'PENDING',$6,$2::date,$3::date,$4
 ON CONFLICT(company_id) DO UPDATE SET status='PENDING',reason=EXCLUDED.reason,requested_at=now(),
 generation=finance_mapping667.queue.generation+1,start_date=EXCLUDED.start_date,end_date=EXCLUDED.end_date,
 annual_only=EXCLUDED.annual_only,refresh_requested=FALSE RETURNING company_id,generation)
 INSERT INTO finance_mapping_ops668.run_item(run_id,company_id,generation) SELECT $5,company_id,generation FROM queued`, ids, req.StartDate, end, req.AnnualOnly, id, queueReason(req.Params))
	return id, e
}

const runSQL = `SELECT r.id,r.parameters,r.requested_by,r.created_at,r.schedule_id,count(i.company_id),count(x.id),
 count(*) FILTER(WHERE x.id IS NOT NULL AND (COALESCE((x.summary->'results'->>'HELD')::int,0)>0 OR x.summary->'results' ? 'HELD_COMPANY_BUDGET' OR x.summary->'results' ? 'HELD_COMPANY_IDENTITY')),
 COALESCE(sum((x.summary->'results'->>'INSERTED')::bigint),0),COALESCE(sum((x.summary->'results'->>'EXISTING')::bigint),0),COALESCE(sum((x.summary->'results'->>'HELD')::bigint),0),
 count(*) FILTER(WHERE x.id IS NULL AND q.generation>i.generation)
 FROM finance_mapping_ops668.run r JOIN finance_mapping_ops668.run_item i ON i.run_id=r.id
 LEFT JOIN finance_mapping667.receipt x ON x.company_id=i.company_id AND x.queue_generation=i.generation
 LEFT JOIN finance_mapping667.queue q ON q.company_id=i.company_id `

func scanRun(row pgx.Row) (Run, error) {
	var v Run
	var body []byte
	e := row.Scan(&v.ID, &body, &v.RequestedBy, &v.CreatedAt, &v.ScheduleID, &v.Companies, &v.Completed, &v.ReviewCompanies, &v.Inserted, &v.Existing, &v.Held, &v.Unverified)
	if e == nil {
		e = json.Unmarshal(body, &v.Parameters)
	}
	v.Pending = v.Companies - v.Completed
	return v, e
}
func (s *Service) run(ctx context.Context, id int64) (Run, error) {
	return scanRun(s.pool.QueryRow(ctx, runSQL+" WHERE r.id=$1 GROUP BY r.id,r.parameters,r.requested_by,r.created_at,r.schedule_id", id))
}
func (s *Service) State(ctx context.Context) (State, error) {
	v := State{Runs: []Run{}, Schedules: []Schedule{}, Held: []Held{}, CheckedAt: s.now().UTC()}
	if e := s.pool.QueryRow(ctx, "SELECT count(*) FROM finance_mapping667.queue WHERE status='PENDING'").Scan(&v.PendingCompanies); e != nil {
		return v, e
	}
	rows, e := s.pool.Query(ctx, strings.Replace(runSQL, "FROM finance_mapping_ops668.run r", "FROM (SELECT * FROM finance_mapping_ops668.run ORDER BY id DESC LIMIT 20) r", 1)+" GROUP BY r.id,r.parameters,r.requested_by,r.created_at,r.schedule_id ORDER BY r.id DESC")
	if e != nil {
		return v, e
	}
	for rows.Next() {
		x, e := scanRun(rows)
		if e != nil {
			rows.Close()
			return v, e
		}
		v.Runs = append(v.Runs, x)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return v, e
	}
	rows, e = s.pool.Query(ctx, "SELECT id,name,cron_expression,timezone,parameters,enabled,next_run_at,last_run_at,last_status FROM finance_mapping_ops668.schedule ORDER BY id DESC LIMIT 100")
	if e != nil {
		return v, e
	}
	for rows.Next() {
		var x Schedule
		var body []byte
		if e = rows.Scan(&x.ID, &x.Name, &x.Cron, &x.Timezone, &body, &x.Enabled, &x.NextRun, &x.LastRun, &x.LastStatus); e != nil {
			rows.Close()
			return v, e
		}
		if e = json.Unmarshal(body, &x.Parameters); e != nil {
			rows.Close()
			return v, e
		}
		v.Schedules = append(v.Schedules, x)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return v, e
	}
	rows, e = s.pool.Query(ctx, "SELECT id,company_id,metric_id,period_end::text,reason FROM finance_mapping667.held ORDER BY id DESC LIMIT 25")
	if e != nil {
		return v, e
	}
	defer rows.Close()
	for rows.Next() {
		var h Held
		if e = rows.Scan(&h.ID, &h.CompanyID, &h.MetricID, &h.End, &h.Reason); e != nil {
			return v, e
		}
		v.Held = append(v.Held, h)
	}
	return v, rows.Err()
}
func (s *Service) SaveSchedule(ctx context.Context, id int64, req ScheduleRequest) (Schedule, error) {
	if e := s.allowMode(req.Parameters); e != nil {
		return Schedule{}, e
	}
	if e := Validate(&req.Parameters, s.now()); e != nil {
		return Schedule{}, e
	}
	n, e := next(req, s.now())
	if e != nil {
		return Schedule{}, e
	}
	var when *time.Time
	if req.Enabled {
		when = &n
	}
	body, _ := json.Marshal(req.Parameters)
	if id == 0 {
		e = s.pool.QueryRow(ctx, "INSERT INTO finance_mapping_ops668.schedule(name,cron_expression,timezone,parameters,enabled,next_run_at) VALUES($1,$2,$3,$4,$5,$6) RETURNING id", req.Name, req.Cron, req.Timezone, body, req.Enabled, when).Scan(&id)
	} else {
		var tag pgconn.CommandTag
		tag, e = s.pool.Exec(ctx, "UPDATE finance_mapping_ops668.schedule SET name=$2,cron_expression=$3,timezone=$4,parameters=$5,enabled=$6,next_run_at=$7 WHERE id=$1", id, req.Name, req.Cron, req.Timezone, body, req.Enabled, when)
		if e == nil && tag.RowsAffected() != 1 {
			return Schedule{}, store.ErrNotFound
		}
	}
	if e != nil {
		return Schedule{}, e
	}
	return Schedule{ScheduleRequest: req, ID: id, NextRun: when}, nil
}
func (s *Service) RunDueSchedules(ctx context.Context, limit int) (int, error) {
	count := 0
	for j := 0; j < limit; j++ {
		did, e := s.tick(ctx)
		if e != nil {
			return count, mapped(e)
		}
		if !did {
			break
		}
		count++
	}
	return count, nil
}
func (s *Service) tick(ctx context.Context) (bool, error) {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return false, e
	}
	defer tx.Rollback(ctx)
	now := s.now().UTC()
	var id int64
	var req ScheduleRequest
	var body []byte
	var due time.Time
	e = tx.QueryRow(ctx, "SELECT id,name,cron_expression,timezone,parameters,next_run_at FROM finance_mapping_ops668.schedule WHERE enabled AND next_run_at<=$1 ORDER BY next_run_at,id FOR UPDATE SKIP LOCKED LIMIT 1", now).Scan(&id, &req.Name, &req.Cron, &req.Timezone, &body, &due)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if e = json.Unmarshal(body, &req.Parameters); e != nil {
		return false, e
	}
	if e = Validate(&req.Parameters, now); e != nil {
		return false, e
	}
	n, e := next(req, now)
	if e != nil {
		return false, e
	}
	_, e = s.enqueue(ctx, tx, Request{Params: req.Parameters, RequestKey: fmt.Sprintf("schedule:%d:%d", id, due.Unix())}, "schedule", &id)
	status := "QUEUED"
	if errors.Is(e, store.ErrConflict) {
		status = "SKIPPED_BUSY"
	} else if e != nil {
		return false, e
	}
	_, e = tx.Exec(ctx, "UPDATE finance_mapping_ops668.schedule SET next_run_at=$2,last_run_at=$3,last_status=$4 WHERE id=$1", id, n, now, status)
	if e != nil {
		return false, e
	}
	return true, tx.Commit(ctx)
}

type Company struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	CIK  string `json:"cik"`
}

func (s *Service) Companies(ctx context.Context, query string) ([]Company, error) {
	query = strings.TrimSpace(query)
	if len(query) < 2 || len(query) > 80 {
		return nil, bad("기업명 또는 정확한 티커를 2~80자로 입력하세요")
	}
	rows, e := s.pool.Query(ctx, `SELECT c.id,c.company_name,c.cik FROM public.company c WHERE strpos(lower(c.company_name),lower($1))>0 OR EXISTS(SELECT 1 FROM public.asset a WHERE a.company_id=c.id AND a.active AND upper(a.ticker)=upper($1)) ORDER BY c.company_name,c.id LIMIT 20`, query)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	v := []Company{}
	for rows.Next() {
		var x Company
		if e = rows.Scan(&x.ID, &x.Name, &x.CIK); e != nil {
			return nil, e
		}
		v = append(v, x)
	}
	return v, rows.Err()
}

type Item struct {
	ReviewCandidates []json.RawMessage `json:"reviewCandidates,omitempty"`
	CompanyID        int64             `json:"companyId"`
	Generation       int64             `json:"generation"`
	ReceiptID        *int64            `json:"receiptId"`
	Results          map[string]int    `json:"results"`
	Excluded         map[string]int    `json:"excluded"`
	State            string            `json:"state"`
}

func (s *Service) Items(ctx context.Context, id int64, offset int) ([]Item, error) {
	if offset < 0 || offset > 5000 {
		return nil, bad("페이지 범위를 확인하세요")
	}
	var exists bool
	if e := s.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM finance_mapping_ops668.run WHERE id=$1)", id).Scan(&exists); e != nil {
		return nil, e
	}
	if !exists {
		return nil, store.ErrNotFound
	}
	rows, e := s.pool.Query(ctx, `SELECT i.company_id,i.generation,x.id,COALESCE(x.summary->'results','{}'::jsonb),COALESCE(x.summary->'excluded','{}'::jsonb),q.generation,COALESCE(x.summary->'reviewCandidates','[]'::jsonb) FROM finance_mapping_ops668.run_item i LEFT JOIN finance_mapping667.receipt x ON x.company_id=i.company_id AND x.queue_generation=i.generation LEFT JOIN finance_mapping667.queue q ON q.company_id=i.company_id WHERE i.run_id=$1 ORDER BY i.company_id LIMIT 50 OFFSET $2`, id, offset)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Item{}
	for rows.Next() {
		var x Item
		var results, excluded, candidates []byte
		var current *int64
		if e = rows.Scan(&x.CompanyID, &x.Generation, &x.ReceiptID, &results, &excluded, &current, &candidates); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(results, &x.Results); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(excluded, &x.Excluded); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(candidates, &x.ReviewCandidates); e != nil {
			return nil, e
		}
		x.State = "PENDING"
		if x.ReceiptID != nil {
			x.State = "COMPLETED"
			if len(x.Results) == 0 {
				x.State = "NO_ELIGIBLE_FACTS"
			}
			for key, n := range x.Results {
				if strings.HasPrefix(key, "HELD") && n > 0 {
					x.State = "REVIEW_REQUIRED"
				}
			}
		} else if current != nil && *current > x.Generation {
			x.State = "UNVERIFIED"
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
