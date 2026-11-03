package recoverycoord

import (
	"strings"
	"testing"
)

func TestContracts(t *testing.T) {
	i := Identity{"KIS", "test-account", "asset:1", "price-1D", "2026-01-01", "2026-01-02", strings.Repeat("a", 64)}
	p := Policy{"coord-v1", 10, 10, 2, 60000, 1000, 0, 1, 100, 100}
	if i.validate() != nil || p.validate() != nil || len(i.Key()) != 64 {
		t.Fatal("valid contract rejected")
	}
	bad := i
	bad.End = "2025-01-01"
	if bad.validate() == nil {
		t.Fatal("invalid range")
	}
	bad = i
	bad.QuotaScope = "secret\nheader"
	if bad.validate() == nil {
		t.Fatal("invalid identifier")
	}
	for _, change := range []func(*Identity){func(i *Identity) { i.Provider = "kis" }, func(i *Identity) { i.Target = "NVDA" }, func(i *Identity) { i.Target = "asset:01" }, func(i *Identity) { i.Revision = "new-job-123" }} {
		q := i
		change(&q)
		if q.validate() == nil {
			t.Fatal("non canonical logical identity")
		}
	}
	sec := i
	sec.Provider = "SEC"
	sec.Target = "company:42"
	if sec.validate() != nil {
		t.Fatal("valid SEC identity rejected")
	}
	for _, change := range []func(*Policy){func(p *Policy) { p.MaxRetries = 11 }, func(p *Policy) { p.LeaseMS = 0 }, func(p *Policy) { p.MaxElapsedMS = 1 }, func(p *Policy) { p.Version = "future" }, func(p *Policy) { p.MaxClaims = 0 }} {
		q := p
		change(&q)
		if q.validate() == nil {
			t.Fatal("bad policy accepted")
		}
	}
	if nextPhase("FETCH_RAW", "COMPLETE") || nextPhase("x", "") {
		t.Fatal("phase skipped")
	}
}
