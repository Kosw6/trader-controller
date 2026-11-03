package newscollection

import (
	"encoding/json"
	"testing"
	"time"
)

var instant = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

func TestRollingDaysAreAnchoredToScheduledUTCInstant(t *testing.T) {
	a, e := Build(Request{Provider: "RSS", Queries: []string{"NVIDIA earnings", "Federal Reserve"}, LookbackDays: 2}, instant)
	if e != nil || len(a.ItemKeys) != 4 {
		t.Fatalf("%+v %v", a, e)
	}
	if a.ItemKeys[0][len(a.ItemKeys[0])-10:] != "2026-10-30" || a.ItemKeys[2][len(a.ItemKeys[2])-10:] != "2026-10-31" {
		t.Fatal(a.ItemKeys)
	}
	b, _ := Build(Request{Provider: "RSS", Queries: []string{"NVIDIA earnings", "Federal Reserve"}, LookbackDays: 2}, instant)
	if string(a.Parameters) != string(b.Parameters) || a.ItemKeys[0] != b.ItemKeys[0] {
		t.Fatal("not deterministic")
	}
	if e = Validate(a.JobType, a.Source, a.ItemKeys, a.Parameters); e != nil {
		t.Fatal(e)
	}
}

func TestGKGPlansEveryQuarterHourAndSupportsHistoricalDays(t *testing.T) {
	p, e := Build(Request{Provider: "GDELT", StartDate: "2016-12-14", EndDate: "2016-12-14"}, instant)
	if e != nil || len(p.ItemKeys) != 96 || p.ItemKeys[0] != "GKG:20161214000000" || p.ItemKeys[95] != "GKG:20161214234500" {
		t.Fatalf("%+v %v", p, e)
	}
	if e = Validate(p.JobType, p.Source, p.ItemKeys, p.Parameters); e != nil {
		t.Fatal(e)
	}
}

func TestRejectsUnboundedFutureAmbiguousAndDuplicatePlans(t *testing.T) {
	for _, r := range []Request{
		{Provider: "OTHER", LookbackDays: 1}, {Provider: "GDELT", LookbackDays: 8},
		{Provider: "GDELT", StartDate: "2016-01-01", EndDate: "2016-01-08"},
		{Provider: "GDELT", StartDate: "2026-11-01", EndDate: "2026-11-01"},
		{Provider: "RSS", Queries: []string{"NVIDIA before:2020-01-01"}, LookbackDays: 1},
		{Provider: "RSS", Queries: []string{"NVIDIA", "NVIDIA"}, LookbackDays: 1},
		{Provider: "RSS", Queries: []string{"a\nb"}, LookbackDays: 1},
		{Provider: "GDELT", Queries: []string{"NVIDIA"}, LookbackDays: 1},
		{Provider: "GDELT", LookbackDays: 1, StartDate: "2016-01-01"},
	} {
		if _, e := Build(r, instant); e == nil {
			t.Fatalf("accepted %+v", r)
		}
	}
}

func TestRejectsTamperedContractAndQueryHash(t *testing.T) {
	p, _ := Build(Request{Provider: "RSS", Queries: []string{"NVIDIA"}, LookbackDays: 1}, instant)
	if Validate(p.JobType, "SEC", p.ItemKeys, p.Parameters) == nil {
		t.Fatal("source mismatch")
	}
	var v Parameters
	json.Unmarshal(p.Parameters, &v)
	for k := range v.Queries {
		v.Queries[k] = "Tesla"
	}
	p.Parameters, _ = json.Marshal(v)
	if Validate(p.JobType, p.Source, p.ItemKeys, p.Parameters) == nil {
		t.Fatal("query tamper")
	}
}
