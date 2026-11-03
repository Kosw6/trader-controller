package recoverycoord_test

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/kosw006/trader-controller/internal/httpapi"
)

func TestPythonCollectorBridge(t *testing.T) {
	python := os.Getenv("RECOVERY_TEST_PYTHON")
	if python == "" {
		t.Skip("explicit Python bridge runtime not configured")
	}
	f := setup(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth := httpapi.AuthConfig{AdminAPIKey: "test-only"}
	a := httptest.NewServer(httpapi.New(nil, auth, log, httpapi.WithRecoveryCoordinator(f.a)).Handler())
	defer a.Close()
	b := httptest.NewServer(httpapi.New(nil, auth, log, httpapi.WithRecoveryCoordinator(f.b)).Handler())
	defer b.Close()
	data, err := filepath.Abs(filepath.Join("..", "..", "..", "trader-data"))
	must(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	files := []string{"-B", "-m", "pytest", "tests/test_durable_integration.py", "tests/test_coordinated_stages.py", "tests/test_coordinated_financial.py", "tests/test_coordinated_continuation.py", "tests/test_financial_revision.py"}
	if os.Getenv("RECOVERY_REAL_SMOKE") == "approved-small-453" || os.Getenv("RECOVERY_REAL_SMOKE") == "approved-kis-455" {
		files = []string{"-B", "-m", "pytest", "tests/test_coordinated_real_smoke.py"}
	}
	files = append(files, "-q", "--tb=short", "--basetemp", filepath.Join(t.TempDir(), "pytest"))
	cmd := exec.CommandContext(ctx, python, files...)
	cmd.Dir = data
	cmd.Env = append(os.Environ(), "RECOVERY_TEST_URL="+a.URL, "RECOVERY_TEST_URL_B="+b.URL,
		"RECOVERY_TEST_SCHEMA="+f.pool.Config().ConnConfig.RuntimeParams["search_path"],
		"PYTHONPATH="+data+string(os.PathListSeparator)+filepath.Join(data, ".cache", "test_deps")+string(os.PathListSeparator)+filepath.Join(data, ".cache", "python-test-deps"))
	output, err := cmd.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		t.Fatalf("Python integration failed: %v", err)
	}
}
