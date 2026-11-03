package httpapi

import (
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kosw006/trader-controller/internal/recoverycoord"
)

func TestCoordinatorDisabledAndAuthBoundaries(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tc := range []struct {
		name, key, body string
		enabled         bool
		status          int
	}{
		{"disabled", "test-only", `{}`, false, 404},
		{"unauthenticated", "", `{}`, true, 401},
		{"invalid json", "test-only", `{`, true, 400},
		{"invalid contract", "test-only", `{}`, true, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opts []Option
			if tc.enabled {
				opts = append(opts, WithRecoveryCoordinator(recoverycoord.New(nil)))
			}
			s := New(nil, AuthConfig{AdminAPIKey: "test-only"}, logger, opts...)
			r := httptest.NewRequest("POST", "/api/v1/admin/pipeline/recovery/work", strings.NewReader(tc.body))
			r.Header.Set("X-Admin-API-Key", tc.key)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status %d body %s", w.Code, w.Body.String())
			}
		})
	}
}
func TestCoordinatorErrorsNeverExposeDatabaseDetails(t *testing.T) {
	w := httptest.NewRecorder()
	coordRespond(w, nil, errors.New("password=private SQL query"))
	if w.Code != 503 || strings.Contains(w.Body.String(), "private") || strings.Contains(w.Body.String(), "SQL") {
		t.Fatal(w.Body.String())
	}
	w = httptest.NewRecorder()
	coordRespond(w, map[string]bool{"sendOnce": true}, recoverycoord.ErrReserved)
	if w.Code != 409 || strings.Contains(w.Body.String(), "sendOnce") {
		t.Fatal("failed reservation granted send")
	}
}
