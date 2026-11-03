package newscollection

import (
	"encoding/json"
	"github.com/kosw006/trader-controller/internal/domain"
	"strings"
	"testing"
)

func selectedRequest(provider string) Request {
	target, _ := CatalogTarget(domain.AssetOption{ID: 42, Ticker: "NVDA", Name: "NVIDIA CORP", Exchange: "NASDAQ", AssetType: "EQUITY"})
	return Request{Provider: provider, AssetIDs: []int64{42}, Targets: []Target{target}, StartDate: "2016-12-30", EndDate: "2016-12-30"}
}

func TestSelectedRSSUsesCatalogNameAndTickerWithV2Snapshot(t *testing.T) {
	plan, err := Build(selectedRequest("RSS"), instant)
	if err != nil {
		t.Fatal(err)
	}
	var p Parameters
	json.Unmarshal(plan.Parameters, &p)
	if p.EnrichmentPolicy != EnrichmentPolicy {
		t.Fatal("selected plans must pin the title/relevance policy")
	}
	if p.Contract != TargetVersion || len(p.Targets) != 1 || len(plan.ItemKeys) != 1 || p.Queries[p.Targets[0].QueryKey] != `("nvidia" OR "NVDA stock")` {
		t.Fatalf("%+v", p)
	}
	if err = Validate(plan.JobType, plan.Source, plan.ItemKeys, plan.Parameters); err != nil {
		t.Fatal(err)
	}
	p.Targets[0].QueryKey = "forged"
	raw, _ := json.Marshal(p)
	if Validate(plan.JobType, plan.Source, plan.ItemKeys, raw) == nil {
		t.Fatal("forged query accepted")
	}
}

func TestEnrichmentPolicyRejectsUnknownVersionAndKeepsOldJobsReadable(t *testing.T) {
	plan, err := Build(selectedRequest("GDELT"), instant)
	if err != nil {
		t.Fatal(err)
	}
	var p Parameters
	json.Unmarshal(plan.Parameters, &p)
	p.EnrichmentPolicy = "unreviewed-policy"
	raw, _ := json.Marshal(p)
	if Validate(plan.JobType, plan.Source, plan.ItemKeys, raw) == nil {
		t.Fatal("unknown enrichment policy accepted")
	}
	p.EnrichmentPolicy = ""
	raw, _ = json.Marshal(p)
	if err = Validate(plan.JobType, plan.Source, plan.ItemKeys, raw); err != nil {
		t.Fatal("legacy plan rejected", err)
	}
}

func TestKoreanDisplayNameUsesLinkedEnglishCompanyForBothProviders(t *testing.T) {
	target, err := CatalogTarget(domain.AssetOption{ID: 2872, Ticker: "NVDA", Name: "엔비디아", CompanyName: " NVIDIA CORP ", Exchange: "NASDAQ", AssetType: "EQUITY"})
	if err != nil || target.Name != "엔비디아" || target.CompanyName != "NVIDIA CORP" || target.OrganizationNames[0] != "nvidia" {
		t.Fatalf("%+v %v", target, err)
	}
	for _, provider := range []string{"RSS", "GDELT"} {
		r := Request{Provider: provider, AssetIDs: []int64{2872}, Targets: []Target{target}, StartDate: "2016-12-30", EndDate: "2016-12-30"}
		p, err := Build(r, instant)
		if err != nil {
			t.Fatal(err)
		}
		if err = Validate(p.JobType, p.Source, p.ItemKeys, p.Parameters); err != nil {
			t.Fatal(err)
		}
		if provider == "RSS" && !strings.Contains(string(p.Parameters), "nvidia") {
			t.Fatal("English query missing")
		}
	}
}

func TestSelectedGDELTDoesNotPretendArchiveDownloadsAreStockSpecific(t *testing.T) {
	plan, err := Build(selectedRequest("GDELT"), instant)
	if err != nil {
		t.Fatal(err)
	}
	var p Parameters
	json.Unmarshal(plan.Parameters, &p)
	if len(plan.ItemKeys) != 96 || len(p.Queries) != 0 || len(p.Targets) != 1 || p.Contract != TargetVersion {
		t.Fatalf("%+v", p)
	}
	if err = Validate(plan.JobType, plan.Source, plan.ItemKeys, plan.Parameters); err != nil {
		t.Fatal(err)
	}
}

func TestUnresolvedMixedOrForgedSelectionRejected(t *testing.T) {
	for _, change := range []func(*Request){
		func(r *Request) { r.AssetIDs = []int64{}; r.Targets = nil },
		func(r *Request) { r.Targets = nil }, func(r *Request) { r.AssetIDs = []int64{42, 42} },
		func(r *Request) { r.Queries = []string{"arbitrary"} },
		func(r *Request) { r.Targets[0].OrganizationNames = []string{"tesla"} },
	} {
		r := selectedRequest("RSS")
		change(&r)
		if _, err := Build(r, instant); err == nil {
			t.Fatalf("accepted %+v", r)
		}
	}
	if _, err := CatalogTarget(domain.AssetOption{ID: 1, Ticker: "A", Name: "Agilent Technologies Inc.", Exchange: "NYSE", AssetType: "ETF"}); err == nil {
		t.Fatal("not equity")
	}
	target, err := CatalogTarget(domain.AssetOption{ID: 2, Ticker: "ON", Name: "ON Semiconductor Corporation", Exchange: "NASDAQ", AssetType: "EQUITY"})
	if err != nil {
		t.Fatal(err)
	}
	if TargetQuery(target) != `("on semiconductor" OR "ON stock")` {
		t.Fatal(TargetQuery(target))
	}
}
