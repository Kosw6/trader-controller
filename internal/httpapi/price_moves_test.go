package httpapi

import (
	"context"
	"github.com/kosw006/trader-controller/internal/domain"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
)

type priceMoveAPI struct {
	newsAPIService
	saves int
}

func (s *priceMoveAPI) GetPriceMoveRule(context.Context) (domain.PriceMoveRule, error) {
	return domain.PriceMoveRule{RisePercent: 5, FallPercent: 5, Version: 1}, nil
}
func (s *priceMoveAPI) SavePriceMoveRule(_ context.Context, r domain.PriceMoveRule, _ string) (domain.PriceMoveRule, error) {
	s.saves++
	return r, nil
}
func (s *priceMoveAPI) ListPriceMoves(context.Context, domain.PriceMoveFilter) (domain.PriceMoveResult, error) {
	return domain.PriceMoveResult{Rows: []domain.PriceMove{}}, nil
}
func TestPriceMovesRoutesPreserveAdminBoundary(t *testing.T) {
	svc := &priceMoveAPI{}
	server := New(svc, AuthConfig{AdminAPIKey: "test-key"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, tc := range []struct {
		method, path, body string
		auth               bool
		status             int
	}{
		{"GET", "/api/v1/admin/price-moves/rule", "", false, 401}, {"GET", "/api/v1/admin/price-moves", "", false, 401},
		{"PUT", "/api/v1/admin/price-moves/rule", `{"risePercent":5,"fallPercent":5,"version":1}`, false, 401},
		{"GET", "/api/v1/admin/price-moves/rule", "", true, 200}, {"GET", "/api/v1/admin/price-moves?offset=oops", "", true, 400},
		{"PUT", "/api/v1/admin/price-moves/rule", `{"risePercent":7,"fallPercent":4,"version":1}`, true, 200},
	} {
		r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		if tc.auth {
			r.Header.Set("X-Admin-API-Key", "test-key")
		}
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s: %d %s", tc.path, w.Code, w.Body.String())
		}
	}
	if svc.saves != 1 {
		t.Fatal("unauthorized update reached service")
	}
}
