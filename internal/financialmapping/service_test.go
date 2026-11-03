package financialmapping

import (
	"errors"
	"github.com/kosw006/trader-controller/internal/store"
	"testing"
	"time"
)

func TestValidateBoundedScope(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		p    Params
		bad  bool
	}{
		{"specific", Params{CompanyIDs: []int64{1440}, StartDate: "2014-01-01", AnnualOnly: true}, false},
		{"all", Params{AllCompanies: true, StartDate: "2014-01-01"}, false},
		{"neither", Params{StartDate: "2014-01-01"}, true},
		{"both", Params{AllCompanies: true, CompanyIDs: []int64{1}, StartDate: "2014-01-01"}, true},
		{"duplicate", Params{CompanyIDs: []int64{1, 1}, StartDate: "2014-01-01"}, true},
		{"negative", Params{CompanyIDs: []int64{-1}, StartDate: "2014-01-01"}, true},
		{"future", Params{AllCompanies: true, StartDate: "2014-01-01", EndDate: "2099-01-01"}, true},
		{"reverse", Params{AllCompanies: true, StartDate: "2025-01-01", EndDate: "2024-01-01"}, true},
		{"malformed", Params{AllCompanies: true, StartDate: "2024-02-30"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := Validate(&tc.p, now)
			if (e != nil) != tc.bad {
				t.Fatalf("got %v", e)
			}
			if e != nil && !errors.Is(e, store.ErrValidation) {
				t.Fatal(e)
			}
		})
	}
	p := Params{AllCompanies: false, CompanyIDs: make([]int64, 5001), StartDate: "2014-01-01"}
	if Validate(&p, now) == nil {
		t.Fatal("unbounded IDs accepted")
	}
	p = Params{CompanyIDs: []int64{3, 1, 2}, StartDate: "2014-01-01"}
	if Validate(&p, now) != nil || p.CompanyIDs[0] != 1 {
		t.Fatal("stable identity required")
	}
}
func TestCronUsesTimezoneAndSkipsMissedSlots(t *testing.T) {
	now := time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC)
	req := ScheduleRequest{Name: "weekly", Cron: "0 9 * * 1", Timezone: "Asia/Seoul"}
	n, e := next(req, now)
	if e != nil || n.UTC() != time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC) {
		t.Fatalf("%v %v", n, e)
	}
	for _, cron := range []string{"* * * * *", "@every 1h", "bad", "0 0 0 * * *"} {
		req.Cron = cron
		if _, e = next(req, now); e == nil {
			t.Fatal("unsafe cron accepted", cron)
		}
	}
	req.Cron = "0 9 * * *"
	req.Timezone = "invalid"
	if _, e = next(req, now); e == nil {
		t.Fatal("unknown timezone accepted")
	}
}
