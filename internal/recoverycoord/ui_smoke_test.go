package recoverycoord_test

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kosw006/trader-controller/internal/app"
	"github.com/kosw006/trader-controller/internal/httpapi"
	"github.com/kosw006/trader-controller/internal/postgres"
	"golang.org/x/crypto/bcrypt"
)

// Explicit opt-in browser harness. Only isolated setup() DB and loopback HTTP.
func TestRecoveryBrowser455(t *testing.T) {
	if os.Getenv("RECOVERY_BROWSER_SMOKE") != "approved-isolated-455" {
		t.Skip("manual browser gate")
	}
	f := setup(t)
	for _, name := range []string{"V016__create_admin_session.sql", "V045__coordinated_stage_evidence.sql"} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "db", "migration", name))
		must(t, err)
		_, err = f.pool.Exec(ctx, string(raw))
		must(t, err)
	}
	_, err := f.pool.Exec(ctx, `CREATE TABLE pipeline_outbox(id bigint,status text);
 CREATE TABLE pipeline_recovery_raw_intent(work_key text,outbox_id bigint);`)
	must(t, err)
	for i, state := range []string{"READY", "REVIEW", "COMPLETE"} {
		id := f.id
		id.Target = []string{"asset:45501", "asset:45502", "asset:45503"}[i]
		w, e := f.a.Register(ctx, id, f.p)
		must(t, e)
		_, e = f.pool.Exec(ctx, "UPDATE pipeline_recovery_work SET state=$2 WHERE work_key=$1", w.Key, state)
		must(t, e)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte("isolated-455-only"), bcrypt.MinCost)
	must(t, err)
	api := httpapi.New(app.New(postgres.New(f.pool)), httpapi.AuthConfig{AdminUsername: "qa455", AdminPasswordHash: string(hash), CookieName: "trader_admin_session_455", SessionTTL: 15 * time.Minute}, slog.New(slog.NewTextHandler(io.Discard, nil)), httpapi.WithRecoveryCoordinator(f.a))
	handler := api.Handler()
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/v1/admin/auth/") && !strings.HasPrefix(r.URL.Path, "/api/v1/admin/pipeline/recovery/") {
			http.Error(w, "outside isolated smoke scope", 503)
			return
		}
		handler.ServeHTTP(w, r)
	}), ReadHeaderTimeout: 5 * time.Second}
	listener, err := net.Listen("tcp", "127.0.0.1:58090")
	must(t, err)
	defer server.Close()
	go server.Serve(listener)
	t.Log("Browser test API ready on loopback 58090; synthetic data only")
	stop := filepath.Join("..", "..", "..", "output", "coordination455-ui-stop")
	deadline := time.Now().Add(15 * time.Minute)
	for time.Now().Before(deadline) {
		if _, err = os.Stat(stop); err == nil {
			return
		}
		time.Sleep(time.Second)
	}
}
