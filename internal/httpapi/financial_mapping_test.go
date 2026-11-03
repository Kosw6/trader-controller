package httpapi

import (
	"context"
	"github.com/kosw006/trader-controller/internal/financialmapping"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type mappingStub struct{ called bool }

func (m *mappingStub) State(context.Context) (financialmapping.State, error) {
	m.called = true
	return financialmapping.State{}, nil
}
func (m *mappingStub) Companies(context.Context, string) ([]financialmapping.Company, error) {
	m.called = true
	return []financialmapping.Company{}, nil
}
func (m *mappingStub) Start(_ context.Context, _ financialmapping.Request, actor string) (financialmapping.Run, error) {
	m.called = true
	if actor != "admin" && actor != "internal-api-key" {
		panic("untrusted actor")
	}
	return financialmapping.Run{ID: 1}, nil
}
func (m *mappingStub) SaveSchedule(context.Context, int64, financialmapping.ScheduleRequest) (financialmapping.Schedule, error) {
	m.called = true
	return financialmapping.Schedule{}, nil
}
func TestMappingRoutesRequireExistingAdminSession(t *testing.T) {
	for _, method := range []string{"GET", "POST"} {
		for _, token := range []string{"", "expired-session", "valid-session"} {
			m := &mappingStub{}
			srv := New(&aliasService{}, AuthConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithFinancialMapping(m))
			path := "/api/v1/admin/financial-mapping"
			body := ""
			if method == "POST" {
				path += "/runs"
				body = `{"allCompanies":true,"startDate":"2014-01-01","requestKey":"test:668"}`
			}
			req := httptest.NewRequest(method, path, strings.NewReader(body))
			if token != "" {
				req.AddCookie(&http.Cookie{Name: "trader_admin_session", Value: token})
			}
			w := httptest.NewRecorder()
			srv.Handler().ServeHTTP(w, req)
			want := 401
			if token == "valid-session" {
				want = 200
				if method == "POST" {
					want = 202
				}
			}
			if w.Code != want || m.called != (token == "valid-session") {
				t.Fatalf("%s %s: %d", method, token, w.Code)
			}
		}
	}
}
func TestMappingRejectsUnexpectedExecutableFields(t *testing.T) {
	m := &mappingStub{}
	srv := New(&aliasService{}, AuthConfig{AdminAPIKey: "fixture"}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithFinancialMapping(m))
	req := httptest.NewRequest("POST", "/api/v1/admin/financial-mapping/runs", strings.NewReader(`{"command":"DROP TABLE company"}`))
	req.Header.Set("X-Admin-API-Key", "fixture")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != 400 || m.called {
		t.Fatalf("%d", w.Code)
	}
}

func (m *mappingStub) Items(context.Context, int64, int) ([]financialmapping.Item, error) {
	return []financialmapping.Item{}, nil
}
