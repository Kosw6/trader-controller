package financialmapping

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestRawVerifiedRepairGatedBeforeDatabaseAccess(t *testing.T) {
	s := &Service{}
	p := Params{Mode: "rawVerifiedRepair"}
	if _, e := s.Start(context.Background(), Request{Params: p}, "test"); e == nil {
		t.Fatal("disabled manual mode accepted")
	}
	if _, e := s.SaveSchedule(context.Background(), 0, ScheduleRequest{Parameters: p}); e == nil {
		t.Fatal("disabled schedule mode accepted")
	}
	s.rawRepairEnabled = true
	if s.allowMode(p) != nil {
		t.Fatal("enabled repair rejected")
	}
}

func TestRawVerifiedRepairRetainsAllPeriodBoundaryAndSchedule(t *testing.T) {
	now := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	for _, annual := range []bool{false, true} {
		p := Params{Mode: "rawVerifiedRepair", CompanyIDs: []int64{3, 6}, StartDate: "2025-01-01", EndDate: "2025-09-30", AnnualOnly: annual}
		if e := Validate(&p, now); e != nil {
			t.Fatal(e)
		}
		if queueReason(p) != "SEC_RAW_REPAIR674" {
			t.Fatal("must consume preverified evidence only")
		}
		b, _ := json.Marshal(ScheduleRequest{Parameters: p})
		var restored ScheduleRequest
		if json.Unmarshal(b, &restored) != nil || queueReason(restored.Parameters) != queueReason(p) || restored.Parameters.AnnualOnly != annual {
			t.Fatal("schedule lost scope")
		}
	}
}

func TestRawVerifiedRepairRejectsBlanketAndUnboundedRequests(t *testing.T) {
	now := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	for _, p := range []Params{
		{Mode: "rawVerifiedRepair", AllCompanies: true, StartDate: "2025-01-01", EndDate: "2025-09-30"},
		{Mode: "rawVerifiedRepair", CompanyIDs: make([]int64, 51), StartDate: "2025-01-01", EndDate: "2025-09-30"},
		{Mode: "rawVerifiedRepair", CompanyIDs: []int64{3}, StartDate: "2025-01-01"},
		{Mode: "rawVerifiedRepair", CompanyIDs: []int64{3, 3}, StartDate: "2025-01-01", EndDate: "2025-09-30"},
	} {
		if Validate(&p, now) == nil {
			t.Fatal("unsafe scope accepted")
		}
	}
}
