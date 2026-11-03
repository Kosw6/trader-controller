package recoverycoord_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kosw006/trader-controller/internal/httpapi"
	c "github.com/kosw006/trader-controller/internal/recoverycoord"
)

var ctx = context.Background()

type fixture struct {
	a, b *c.Coordinator
	pool *pgxpool.Pool
	id   c.Identity
	p    c.Policy
}

func setup(t *testing.T) *fixture {
	t.Helper()
	dsn := os.Getenv("RECOVERY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated DB not configured")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	must(t, err)
	// Refuse production/local-backfill DBs, even when the variable is accidentally set.
	if cfg.ConnConfig.Database != "recovery451_test" || (cfg.ConnConfig.Host != "127.0.0.1" && cfg.ConnConfig.Host != "localhost") || cfg.ConnConfig.Port != 55451 {
		t.Fatal("not the isolated recovery451 database")
	}
	cfg.MaxConns = 4
	admin, err := pgxpool.NewWithConfig(ctx, cfg.Copy())
	must(t, err)
	t.Cleanup(admin.Close)
	schema := fmt.Sprintf("coord_%d", time.Now().UnixNano())
	_, err = admin.Exec(ctx, "CREATE SCHEMA "+schema)
	must(t, err)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	a, err := pgxpool.NewWithConfig(ctx, cfg.Copy())
	must(t, err)
	t.Cleanup(a.Close)
	b, err := pgxpool.NewWithConfig(ctx, cfg.Copy())
	must(t, err)
	t.Cleanup(b.Close)
	migration, err := os.ReadFile(filepath.Join("..", "..", "db", "migration", "V043__pipeline_recovery_coordination.sql"))
	must(t, err)
	_, err = a.Exec(ctx, string(migration))
	must(t, err)
	_, err = a.Exec(ctx, `CREATE TABLE test_effect(work_key text PRIMARY KEY,value text NOT NULL)`)
	must(t, err)
	return &fixture{c.New(a), c.New(b), a, c.Identity{"KIS", "test-quota", "asset:1", "price-1D", "2026-01-01", "2026-01-02", strings.Repeat("a", 64)}, c.Policy{"coord-v1", 20, 10, 2, 60000, 10000, 0, 1, 10, 100}}
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func want(t *testing.T, err, expected error) {
	t.Helper()
	if !errors.Is(err, expected) {
		t.Fatalf("got %v want %v", err, expected)
	}
}
func (f *fixture) start(t *testing.T) (c.Work, c.Lease) {
	t.Helper()
	w, e := f.a.Register(ctx, f.id, f.p)
	must(t, e)
	w, e = f.a.Claim(ctx, w.Key, "worker-a")
	must(t, e)
	return w, c.Lease{WorkKey: w.Key, Owner: w.Owner, Fence: w.Fence}
}
func expire(t *testing.T, f *fixture, key string) {
	t.Helper()
	_, e := f.pool.Exec(ctx, `UPDATE pipeline_recovery_work SET lease_until=clock_timestamp()-interval '1 second' WHERE work_key=$1`, key)
	must(t, e)
}
func receipt() c.Receipt {
	return c.Receipt{ID: "result-1", From: "FETCH_RAW", To: "RAW_READY", Hash: strings.Repeat("a", 64)}
}
func (f *fixture) effect(key, value string) func(context.Context, c.SQL) error {
	return func(ctx context.Context, tx c.SQL) error {
		_, e := tx.Exec(ctx, `INSERT INTO test_effect(work_key,value) VALUES($1,$2)`, key, value)
		return e
	}
}

func TestIntegrationCoordinator(t *testing.T) {
	t.Run("immutable registration survives another controller", func(t *testing.T) {
		f := setup(t)
		w, l := f.start(t)
		must(t, f.a.Reserve(ctx, l, "r1", "page1"))
		must(t, f.a.Report(ctx, l, "r1", "HTTP_OK", 0))
		x, e := f.b.Register(ctx, f.id, f.p)
		must(t, e)
		if x.Requests != 1 || !x.Deadline.Equal(w.Deadline) {
			t.Fatal("budget or deadline reset")
		}
		p := f.p
		p.MaxRequests++
		_, e = f.b.Register(ctx, f.id, p)
		want(t, e, c.ErrConflict)
		i := f.id
		i.Target = "asset:2"
		p = f.p
		p.IntervalMS++
		_, e = f.b.Register(ctx, i, p)
		want(t, e, c.ErrConflict)
	})
	t.Run("two pools twenty four claims one winner", func(t *testing.T) {
		f := setup(t)
		w, e := f.a.Register(ctx, f.id, f.p)
		must(t, e)
		start := make(chan struct{})
		results := make(chan error, 24)
		var wg sync.WaitGroup
		for n := 0; n < 24; n++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				<-start
				client := f.a
				if n%2 == 1 {
					client = f.b
				}
				_, e := client.Claim(ctx, w.Key, fmt.Sprintf("w-%d", n))
				results <- e
			}(n)
		}
		close(start)
		wg.Wait()
		close(results)
		wins := 0
		for e := range results {
			if e == nil {
				wins++
			} else {
				want(t, e, c.ErrLease)
			}
		}
		x, e := f.b.Read(ctx, w.Key)
		must(t, e)
		if wins != 1 || x.Claims != 1 || x.Fence != 1 {
			t.Fatalf("wins %d work %+v", wins, x)
		}
	})
	t.Run("reserve charged once and unknown send blocks takeover", func(t *testing.T) {
		f := setup(t)
		w, l := f.start(t)
		must(t, f.a.Reserve(ctx, l, "r1", "page1"))
		want(t, f.b.Reserve(ctx, l, "r1", "page1"), c.ErrReserved)
		x, e := f.b.Read(ctx, w.Key)
		must(t, e)
		if x.Requests != 1 {
			t.Fatal("duplicate charge")
		}
		r, e := f.b.ReadRequest(ctx, w.Key, "r1")
		must(t, e)
		if r.Outcome != "PENDING" {
			t.Fatal(r)
		}
		expire(t, f, w.Key)
		_, e = f.b.Claim(ctx, w.Key, "worker-b")
		want(t, e, c.ErrReview)
		want(t, f.a.Report(ctx, l, "r1", "HTTP_OK", 0), c.ErrLease)
	})
	t.Run("persistent requests retries cannot reset", func(t *testing.T) {
		f := setup(t)
		f.p.MaxRequests = 2
		f.p.MaxRetries = 1
		w, l := f.start(t)
		must(t, f.a.Reserve(ctx, l, "r1", "page1"))
		must(t, f.a.Report(ctx, l, "r1", "TRANSIENT_TRANSPORT", 0))
		time.Sleep(20 * time.Millisecond)
		must(t, f.a.Pause(ctx, w.Key))
		must(t, f.b.Resume(ctx, w.Key))
		x, e := f.b.Claim(ctx, w.Key, "worker-b")
		must(t, e)
		l = c.Lease{WorkKey: x.Key, Owner: x.Owner, Fence: x.Fence}
		must(t, f.b.Reserve(ctx, l, "r2", "page1"))
		must(t, f.b.Report(ctx, l, "r2", "TRANSIENT_TRANSPORT", 0))
		time.Sleep(20 * time.Millisecond)
		want(t, f.b.Reserve(ctx, l, "r3", "page1"), c.ErrBudget)
		must(t, f.a.Pause(ctx, w.Key))
		want(t, f.b.Resume(ctx, w.Key), c.ErrBudget)
		x, e = f.b.Read(ctx, w.Key)
		must(t, e)
		if x.Requests != 2 || x.Retries != 1 || !x.Deadline.Equal(w.Deadline) {
			t.Fatal("budget reset")
		}
	})
	t.Run("retry limit independent from request limit", func(t *testing.T) {
		f := setup(t)
		f.p.MaxRetries = 0
		_, l := f.start(t)
		must(t, f.a.Reserve(ctx, l, "r1", "page1"))
		must(t, f.a.Report(ctx, l, "r1", "TRANSIENT_TRANSPORT", 0))
		time.Sleep(20 * time.Millisecond)
		want(t, f.b.Reserve(ctx, l, "r2", "page1"), c.ErrBudget)
	})
	t.Run("shared credential cooldown and in flight gate", func(t *testing.T) {
		f := setup(t)
		_, l := f.start(t)
		other := f.id
		other.Target = "asset:2"
		w, e := f.b.Register(ctx, other, f.p)
		must(t, e)
		w, e = f.b.Claim(ctx, w.Key, "worker-b")
		must(t, e)
		l2 := c.Lease{WorkKey: w.Key, Owner: w.Owner, Fence: w.Fence}
		must(t, f.a.Reserve(ctx, l, "r1", "page1"))
		want(t, f.b.Reserve(ctx, l2, "r1", "page1"), c.ErrCooling)
		must(t, f.a.Report(ctx, l, "r1", "RATE_LIMIT", 500))
		want(t, f.b.Reserve(ctx, l2, "r1", "page1"), c.ErrCooling)
		time.Sleep(550 * time.Millisecond)
		must(t, f.b.Reserve(ctx, l2, "r1", "page1"))
	})
	t.Run("elapsed and claim budgets persist", func(t *testing.T) {
		f := setup(t)
		f.p.MaxClaims = 1
		w, l := f.start(t)
		_, e := f.a.Renew(ctx, l)
		must(t, e)
		expire(t, f, w.Key)
		_, e = f.b.Claim(ctx, w.Key, "b")
		want(t, e, c.ErrBudget)
	})
	t.Run("expired lease never revived and stale worker cannot write", func(t *testing.T) {
		f := setup(t)
		f.p.LeaseMS = 200
		w, l := f.start(t)
		time.Sleep(250 * time.Millisecond)
		_, e := f.a.Renew(ctx, l)
		want(t, e, c.ErrLease)
		x, e := f.b.Claim(ctx, w.Key, "b")
		must(t, e)
		if x.Fence <= l.Fence {
			t.Fatal("fence not monotonic")
		}
		called := false
		want(t, f.a.Apply(ctx, l, receipt(), func(context.Context, c.SQL) error { called = true; return nil }), c.ErrLease)
		if called {
			t.Fatal("stale callback ran")
		}
		want(t, f.a.PauseOwned(ctx, l), c.ErrLease)
		current, e := f.b.Read(ctx, w.Key)
		must(t, e)
		if current.State != "LEASED" || current.Fence != x.Fence {
			t.Fatal("stale cancellation stopped new owner")
		}
	})
	t.Run("pause invalidates live worker", func(t *testing.T) {
		f := setup(t)
		w, l := f.start(t)
		must(t, f.b.Pause(ctx, w.Key))
		want(t, f.a.Reserve(ctx, l, "r1", "p1"), c.ErrLease)
		_, e := f.a.Claim(ctx, w.Key, "a")
		want(t, e, c.ErrReview)
		must(t, f.b.Resume(ctx, w.Key))
		x, e := f.b.Claim(ctx, w.Key, "b")
		must(t, e)
		if x.Fence <= l.Fence {
			t.Fatal("pause failed to fence")
		}
	})
	t.Run("non transient classifications require review", func(t *testing.T) {
		for _, outcome := range []string{"AUTH_BLOCKED", "EMPTY_UNRESOLVED", "SOURCE_NOT_FOUND", "QUALITY_OR_SEMANTIC_CONFLICT", "COMMIT_UNCERTAIN", "IMPLEMENTATION_ERROR", "RESOURCE_BLOCKED", "UNCLASSIFIED", "USER_PAUSED"} {
			t.Run(outcome, func(t *testing.T) {
				f := setup(t)
				w, l := f.start(t)
				must(t, f.a.Reserve(ctx, l, "r1", "p1"))
				must(t, f.a.Report(ctx, l, "r1", outcome, 0))
				_, e := f.b.Claim(ctx, w.Key, "b")
				want(t, e, c.ErrReview)
				if outcome != "USER_PAUSED" {
					want(t, f.b.Pause(ctx, w.Key), c.ErrReview)
					want(t, f.b.Resume(ctx, w.Key), c.ErrReview)
				}
			})
		}
	})
	t.Run("atomic data receipt and no refetch after raw", func(t *testing.T) {
		f := setup(t)
		w, l := f.start(t)
		must(t, f.a.Apply(ctx, l, receipt(), f.effect(w.Key, "raw")))
		r, e := f.b.ReadReceipt(ctx, w.Key, "result-1")
		must(t, e)
		if r.Hash != receipt().Hash {
			t.Fatal("receipt mismatch")
		}
		// Simulate caller losing the commit response: new pool reads receipt, no replay.
		x, e := f.b.Claim(ctx, w.Key, "b")
		must(t, e)
		if x.Phase != "RAW_READY" {
			t.Fatal("lost checkpoint")
		}
		l2 := c.Lease{WorkKey: w.Key, Owner: x.Owner, Fence: x.Fence}
		want(t, f.b.Reserve(ctx, l2, "r2", "page1"), c.ErrReview)
		want(t, f.b.Apply(ctx, l2, receipt(), f.effect(w.Key, "duplicate")), c.ErrReceipt)
		var n int
		must(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM test_effect`).Scan(&n))
		if n != 1 {
			t.Fatal("duplicate result")
		}
	})
	t.Run("callback failure rolls back both data and receipt", func(t *testing.T) {
		f := setup(t)
		w, l := f.start(t)
		injected := errors.New("injected")
		e := f.a.Apply(ctx, l, receipt(), func(ctx context.Context, tx c.SQL) error {
			if e := f.effect(w.Key, "raw")(ctx, tx); e != nil {
				return e
			}
			return injected
		})
		want(t, e, injected)
		var n int
		must(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM test_effect`).Scan(&n))
		if n != 0 {
			t.Fatal("partial commit")
		}
		must(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM pipeline_recovery_receipt`).Scan(&n))
		if n != 0 {
			t.Fatal("false receipt")
		}
	})
	t.Run("lease expires during callback rolls back", func(t *testing.T) {
		f := setup(t)
		f.p.LeaseMS = 200
		w, l := f.start(t)
		called := false
		e := f.a.Apply(ctx, l, receipt(), func(ctx context.Context, tx c.SQL) error {
			called = true
			if e := f.effect(w.Key, "raw")(ctx, tx); e != nil {
				return e
			}
			time.Sleep(250 * time.Millisecond)
			return nil
		})
		want(t, e, c.ErrLease)
		if !called {
			t.Fatal("expiry was not injected during callback")
		}
		var n int
		must(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM test_effect`).Scan(&n))
		if n != 0 {
			t.Fatal("expired write committed")
		}
	})
	t.Run("deadline cannot be extended by renewal", func(t *testing.T) {
		f := setup(t)
		w, l := f.start(t)
		_, e := f.pool.Exec(ctx, `UPDATE pipeline_recovery_work SET deadline=clock_timestamp()-interval '1 second' WHERE work_key=$1`, w.Key)
		must(t, e)
		_, e = f.a.Renew(ctx, l)
		want(t, e, c.ErrBudget)
		want(t, f.a.Reserve(ctx, l, "r1", "p1"), c.ErrBudget)
	})
	t.Run("invalid waits and categories do not release request", func(t *testing.T) {
		f := setup(t)
		w, l := f.start(t)
		must(t, f.a.Reserve(ctx, l, "r1", "p1"))
		want(t, f.a.Report(ctx, l, "r1", "RATE_LIMIT", -1), c.ErrInvalid)
		want(t, f.a.Report(ctx, l, "r1", "secret error text", 0), c.ErrInvalid)
		r, e := f.b.ReadRequest(ctx, w.Key, "r1")
		must(t, e)
		if r.Outcome != "PENDING" {
			t.Fatal("released ambiguous request")
		}
	})
	t.Run("simultaneous reservations grant one send", func(t *testing.T) {
		f := setup(t)
		w, l := f.start(t)
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() { <-start; results <- f.a.Reserve(ctx, l, "r1", "page1") }()
		go func() { <-start; results <- f.b.Reserve(ctx, l, "r2", "page1") }()
		close(start)
		wins := 0
		for n := 0; n < 2; n++ {
			e := <-results
			if e == nil {
				wins++
			} else {
				want(t, e, c.ErrCooling)
			}
		}
		x, e := f.a.Read(ctx, w.Key)
		must(t, e)
		if wins != 1 || x.Requests != 1 {
			t.Fatal("multiple send permissions")
		}
	})
	t.Run("simultaneous phase writes commit exactly once", func(t *testing.T) {
		f := setup(t)
		w, l := f.start(t)
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() { <-start; results <- f.a.Apply(ctx, l, receipt(), f.effect(w.Key, "raw")) }()
		go func() { <-start; results <- f.b.Apply(ctx, l, receipt(), f.effect(w.Key, "raw")) }()
		close(start)
		wins := 0
		for n := 0; n < 2; n++ {
			e := <-results
			if e == nil {
				wins++
			} else {
				want(t, e, c.ErrLease)
			}
		}
		var count int
		must(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM test_effect`).Scan(&count))
		if wins != 1 || count != 1 {
			t.Fatal("duplicate phase write")
		}
	})
	t.Run("heartbeats are not progress and complete cannot restart", func(t *testing.T) {
		f := setup(t)
		w, l := f.start(t)
		time.Sleep(5 * time.Millisecond)
		renewed, e := f.b.Renew(ctx, l)
		must(t, e)
		if !renewed.ProgressAt.Equal(w.ProgressAt) || renewed.HeartbeatAt == nil || !renewed.HeartbeatAt.After(*w.HeartbeatAt) {
			t.Fatal("heartbeat/progress conflated")
		}
		phases := []string{"FETCH_RAW", "RAW_READY", "ETL_READY", "CANDIDATE", "VERIFIED", "COMPLETE"}
		for n := 0; n < len(phases)-1; n++ {
			if n > 0 {
				x, e := f.b.Claim(ctx, w.Key, "b")
				must(t, e)
				l = c.Lease{WorkKey: w.Key, Owner: x.Owner, Fence: x.Fence}
			}
			r := receipt()
			r.ID = fmt.Sprintf("phase-%d", n)
			r.From = phases[n]
			r.To = phases[n+1]
			must(t, f.a.Apply(ctx, l, r, func(context.Context, c.SQL) error { return nil }))
		}
		x, e := f.b.Read(ctx, w.Key)
		must(t, e)
		if x.State != "COMPLETE" || x.Phase != "COMPLETE" || x.LeaseUntil != nil {
			t.Fatal("incomplete terminal state")
		}
		_, e = f.a.Claim(ctx, w.Key, "a")
		want(t, e, c.ErrReview)
		want(t, f.a.Pause(ctx, w.Key), c.ErrReview)
	})
	t.Run("two HTTP controllers share same lease ledger", func(t *testing.T) {
		f := setup(t)
		w, e := f.a.Register(ctx, f.id, f.p)
		must(t, e)
		log := slog.New(slog.NewTextHandler(io.Discard, nil))
		auth := httpapi.AuthConfig{AdminAPIKey: "test-only"}
		a := httptest.NewServer(httpapi.New(nil, auth, log, httpapi.WithRecoveryCoordinator(f.a)).Handler())
		defer a.Close()
		b := httptest.NewServer(httpapi.New(nil, auth, log, httpapi.WithRecoveryCoordinator(f.b)).Handler())
		defer b.Close()
		for n, url := range []string{a.URL, b.URL} {
			req, e := http.NewRequest("POST", url+"/api/v1/admin/pipeline/recovery/work/"+w.Key+"/claim", bytes.NewBufferString(`{"owner":"http-worker"}`))
			must(t, e)
			req.Header.Set("X-Admin-API-Key", "test-only")
			res, e := http.DefaultClient.Do(req)
			must(t, e)
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			expected := 200
			if n == 1 {
				expected = 409
			}
			if res.StatusCode != expected {
				t.Fatalf("server %d status %d", n, res.StatusCode)
			}
		}
	})
}
