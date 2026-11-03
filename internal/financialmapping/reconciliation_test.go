package financialmapping

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestReconciliationDisabledBeforeDatabaseAccess(t *testing.T) {
	s := &Service{}
	p := Params{Mode: "reviewedReconciliation"}
	if _, e := s.Start(context.Background(), Request{Params: p}, "test"); e == nil {
		t.Fatal("disabled manual mode accepted")
	}
	if _, e := s.SaveSchedule(context.Background(), 0, ScheduleRequest{Parameters: p}); e == nil {
		t.Fatal("disabled scheduled mode accepted")
	}
	s.reconciliationEnabled = true
	if s.allowMode(p) != nil || (&Service{}).allowMode(Params{}) != nil {
		t.Fatal("mode gate incorrect")
	}
}

func TestReconciliationManualAndScheduleUseSameBoundedMode(t *testing.T) {
	now := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	p := Params{Mode: "reviewedReconciliation", CompanyIDs: []int64{676, 6}, AnnualOnly: true, StartDate: "2016-01-01", EndDate: "2016-12-31"}
	if Validate(&p, now) != nil || queueReason(p) != "REVIEWED_RECONCILIATION677" {
		t.Fatal("review mode not routed")
	}
	body, _ := json.Marshal(ScheduleRequest{Name: "reviewed", Cron: "0 9 * * 1", Timezone: "Asia/Seoul", Parameters: p})
	var restored ScheduleRequest
	if json.Unmarshal(body, &restored) != nil || queueReason(restored.Parameters) != queueReason(p) {
		t.Fatal("scheduled mode lost")
	}
	for _, bad := range []Params{
		{Mode: "unknown", CompanyIDs: []int64{6}, StartDate: "2016-01-01"},
		{Mode: p.Mode, AllCompanies: true, AnnualOnly: true, StartDate: p.StartDate},
		{Mode: p.Mode, CompanyIDs: []int64{6}, StartDate: p.StartDate},
		{Mode: p.Mode, CompanyIDs: make([]int64, 51), AnnualOnly: true, StartDate: p.StartDate},
	} {
		if Validate(&bad, now) == nil {
			t.Fatal("unsafe mode accepted")
		}
	}
	if queueReason(Params{}) != "ADMIN_MAPPING" {
		t.Fatal("legacy mode changed")
	}
	legacy, _ := json.Marshal(Params{CompanyIDs: []int64{6}, StartDate: p.StartDate})
	var fields map[string]any
	json.Unmarshal(legacy, &fields)
	if _, ok := fields["mode"]; ok {
		t.Fatal("default JSON/idempotency changed")
	}
}
