package httpapi

import (
	"context"
	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/store"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type aliasService struct {
	Service
	called           bool
	assetID, aliasID int64
	fail             error
}

func (s *aliasService) GetAdminSession(_ context.Context, token string) (domain.AdminSession, error) {
	if token != "valid-session" {
		return domain.AdminSession{}, store.ErrNotFound
	}
	return domain.AdminSession{Actor: "admin"}, nil
}

func TestAliasRoutesUseExistingSession(t *testing.T) {
	for _, token := range []string{"valid-session", "expired-session"} {
		svc := &aliasService{}
		srv := New(svc, AuthConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		req := httptest.NewRequest("GET", "/api/v1/admin/asset-aliases", nil)
		req.AddCookie(&http.Cookie{Name: "trader_admin_session", Value: token})
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, req)
		want := 401
		if token == "valid-session" {
			want = 200
		}
		if w.Code != want {
			t.Fatalf("session %s status %d", token, w.Code)
		}
	}
}

func (s *aliasService) ListAssetAliases(context.Context, domain.AssetLinkFilter) ([]domain.AssetAliases, error) {
	s.called = true
	return []domain.AssetAliases{}, s.fail
}
func (s *aliasService) SaveAssetAlias(_ context.Context, a, b int64, _ domain.SaveAssetAlias) (domain.AssetAlias, error) {
	s.called = true
	s.assetID = a
	s.aliasID = b
	return domain.AssetAlias{ID: 9, Alias: "엔비"}, s.fail
}
func (s *aliasService) DeleteAssetAlias(_ context.Context, a, b int64) error {
	s.called = true
	s.assetID = a
	s.aliasID = b
	return s.fail
}

func TestAliasRoutes(t *testing.T) {
	for _, tc := range []struct {
		method, path, body string
		auth               bool
		failure            error
		want               int
	}{
		{"GET", "/api/v1/admin/asset-aliases", "", false, nil, 401},
		{"POST", "/api/v1/admin/assets/1/aliases", `{"alias":"엔비"}`, false, nil, 401},
		{"PUT", "/api/v1/admin/assets/1/aliases/9", `{"alias":"엔비"}`, false, nil, 401},
		{"DELETE", "/api/v1/admin/assets/1/aliases/9", "", false, nil, 401},
		{"GET", "/api/v1/admin/asset-aliases?q=NVDA", "", true, nil, 200},
		{"POST", "/api/v1/admin/assets/1/aliases", `{"alias":"엔비"}`, true, nil, 201},
		{"PUT", "/api/v1/admin/assets/1/aliases/9", `{"alias":"엔비"}`, true, nil, 200},
		{"DELETE", "/api/v1/admin/assets/1/aliases/9", "", true, nil, 200},
		{"PUT", "/api/v1/admin/assets/1/aliases/0", `{"alias":"엔비"}`, true, nil, 400},
		{"POST", "/api/v1/admin/assets/1/aliases", `{"alias":"엔비","source":"BAD"}`, true, nil, 400},
		{"POST", "/api/v1/admin/assets/1/aliases", `{"alias":"엔비"}`, true, store.ErrValidation, 400},
		{"POST", "/api/v1/admin/assets/1/aliases", `{"alias":"엔비"}`, true, store.ErrConflict, 409},
		{"DELETE", "/api/v1/admin/assets/1/aliases/9", "", true, store.ErrNotFound, 404},
	} {
		t.Run(tc.method+tc.path+string(rune(tc.want)), func(t *testing.T) {
			svc := &aliasService{fail: tc.failure}
			srv := New(svc, AuthConfig{AdminAPIKey: "secret"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			if tc.auth {
				req.Header.Set("X-Admin-API-Key", "secret")
			}
			w := httptest.NewRecorder()
			srv.Handler().ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("status %d want %d: %s", w.Code, tc.want, w.Body.String())
			}
			if !tc.auth && svc.called {
				t.Fatal("unauthorized request reached service")
			}
			if svc.called && tc.method != "GET" && svc.assetID != 1 {
				t.Fatal("wrong asset binding")
			}
		})
	}
}
