package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kosw006/trader-controller/internal/recovery"
	"github.com/kosw006/trader-controller/internal/store"
)

type recoveryService struct {
	Service
	calls int
	id    int64
	err   error
}

func (s *recoveryService) RecoveryPreview(_ context.Context, id int64) (recovery.Preview, error) {
	s.calls++
	s.id = id
	return recovery.Preview{Version: "recovery-preview-v1", JobID: id}, s.err
}

func TestRecoveryPreviewRoute(t *testing.T) {
	for _, tc := range []struct {
		name, path, key, method string
		err                     error
		status, calls           int
	}{
		{"authorized", "7", "test-key", "GET", nil, 200, 1},
		{"unauthorized", "7", "", "GET", nil, 401, 0},
		{"bad id", "bad", "test-key", "GET", nil, 400, 0},
		{"missing", "7", "test-key", "GET", store.ErrNotFound, 404, 1},
		{"read only", "7", "test-key", "POST", nil, 405, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &recoveryService{err: tc.err}
			s := New(svc, AuthConfig{AdminAPIKey: "test-key"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			r := httptest.NewRequest(tc.method, "/api/v1/admin/pipeline/jobs/"+tc.path+"/recovery-preview", nil)
			r.Header.Set("X-Admin-API-Key", tc.key)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != tc.status || svc.calls != tc.calls {
				t.Fatalf("status %d calls %d body %s", w.Code, svc.calls, w.Body.String())
			}
			if w.Code == http.StatusOK && (!strings.Contains(w.Body.String(), `"executionAllowed":false`) || svc.id != 7) {
				t.Fatal("unsafe or incorrect response")
			}
		})
	}
}
