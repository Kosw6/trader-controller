package app

import (
	"context"
	"github.com/kosw006/trader-controller/internal/domain"
	"math"
	"testing"
)

type moveStore struct {
	stubStore
	saves int
	last  domain.PriceMoveFilter
}

func (m *moveStore) GetPriceMoveRule(context.Context) (domain.PriceMoveRule, error) {
	return domain.PriceMoveRule{RisePercent: 5, FallPercent: 5, Version: 1}, nil
}
func (m *moveStore) SavePriceMoveRule(_ context.Context, r domain.PriceMoveRule, _ string) (domain.PriceMoveRule, error) {
	m.saves++
	r.Version++
	return r, nil
}
func (m *moveStore) ListPriceMoves(_ context.Context, f domain.PriceMoveFilter) (domain.PriceMoveResult, error) {
	m.last = f
	return domain.PriceMoveResult{Rows: []domain.PriceMove{}}, nil
}
func TestPriceMoveRuleRejectsNonfiniteAndInvalidThresholds(t *testing.T) {
	for _, r := range []domain.PriceMoveRule{{RisePercent: 0, FallPercent: 5, Version: 1}, {RisePercent: 5, FallPercent: -5, Version: 1}, {RisePercent: 5, FallPercent: 101, Version: 1}, {RisePercent: math.NaN(), FallPercent: 5, Version: 1}, {RisePercent: 5, FallPercent: math.Inf(1), Version: 1}, {RisePercent: 5.00001, FallPercent: 5, Version: 1}, {RisePercent: 5, FallPercent: 5, Version: 0}} {
		st := &moveStore{}
		s := New(st)
		if _, err := s.SavePriceMoveRule(context.Background(), r, "admin"); err == nil || st.saves != 0 {
			t.Fatalf("accepted invalid rule %#v", r)
		}
	}
	st := &moveStore{}
	r, err := New(st).SavePriceMoveRule(context.Background(), domain.PriceMoveRule{RisePercent: 0.01, FallPercent: 100, Version: 1}, "admin")
	if err != nil || r.Version != 2 || st.saves != 1 {
		t.Fatal(r, err)
	}
}
func TestPriceMoveFiltersAreBounded(t *testing.T) {
	for _, f := range []domain.PriceMoveFilter{{Direction: "DROP TABLE"}, {Date: "invalid", Direction: "ALL"}, {Date: "2099-01-01", Direction: "ALL"}, {Direction: "UP", Offset: -1}, {Direction: "DOWN", Offset: 10001}} {
		if _, err := New(&moveStore{}).ListPriceMoves(context.Background(), f); err == nil {
			t.Fatal(f)
		}
	}
	st := &moveStore{}
	_, err := New(st).ListPriceMoves(context.Background(), domain.PriceMoveFilter{Date: "2026-10-23", Direction: "ALL", Query: " NVDA "})
	if err != nil || st.last.Query != "NVDA" {
		t.Fatal(st.last, err)
	}
}

func TestPriceMoveRangesValidateAndKeepMode(t *testing.T) {
	for _, f := range []domain.PriceMoveFilter{
		{Mode: "EVENTS", Direction: "ALL", Start: "2026-10-01"},
		{Mode: "EVENTS", Direction: "ALL", Start: "2026-10-23", End: "2026-10-01"},
		{Mode: "EVENTS", Direction: "ALL", Start: "2020-01-01", End: "2026-10-23"},
		{Mode: "PERIOD", Direction: "ALL", Start: "2026-10-23", End: "2026-10-23"},
		{Mode: "WRONG", Direction: "ALL"},
		{Mode: "EVENTS", Direction: "ALL", Date: "2026-10-23", Start: "2026-10-01", End: "2026-10-23"},
	} {
		if _, err := New(&moveStore{}).ListPriceMoves(context.Background(), f); err == nil {
			t.Fatal("invalid range accepted", f)
		}
	}
	for _, mode := range []string{"EVENTS", "PERIOD"} {
		st := &moveStore{}
		_, err := New(st).ListPriceMoves(context.Background(), domain.PriceMoveFilter{Mode: mode, Direction: "ALL", Start: "2026-10-01", End: "2026-10-23"})
		if err != nil || st.last.Start != "2026-10-01" || st.last.End != "2026-10-23" || st.last.Mode != mode {
			t.Fatal(st.last, err)
		}
	}
}
