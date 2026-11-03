package newscollection

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"

	"github.com/kosw006/trader-controller/internal/domain"
)

const TargetVersion = "news-v2"

// Target is a catalog snapshot, not a verified link between an article and an exchange security.
type Target struct {
	AssetID           int64    `json:"assetId"`
	Ticker            string   `json:"ticker"`
	Name              string   `json:"name"`
	CompanyName       string   `json:"companyName,omitempty"`
	Exchange          string   `json:"exchange"`
	OrganizationNames []string `json:"organizationNames"`
	QueryKey          string   `json:"queryKey,omitempty"`
}

var punctuation = regexp.MustCompile(`[^\p{L}\p{N} ]+`)
var spaces = regexp.MustCompile(`\s+`)
var tickerPattern = regexp.MustCompile(`^[A-Z0-9][A-Z0-9.\-]{0,19}$`)

func coreName(name string) string {
	words := strings.Fields(spaces.ReplaceAllString(punctuation.ReplaceAllString(strings.ToLower(name), " "), " "))
	suffix := map[string]bool{"inc": true, "incorporated": true, "corp": true, "corporation": true, "ltd": true, "limited": true, "plc": true, "co": true, "company": true}
	for len(words) > 1 && suffix[words[len(words)-1]] {
		words = words[:len(words)-1]
	}
	return strings.Join(words, " ")
}

func CatalogTarget(asset domain.AssetOption) (Target, error) {
	ticker := strings.ToUpper(strings.TrimSpace(asset.Ticker))
	searchName := asset.Name
	companyName := strings.TrimSpace(asset.CompanyName)
	if companyName != "" {
		searchName = companyName
	}
	name := coreName(searchName)
	if asset.ID <= 0 || asset.AssetType != "EQUITY" || !tickerPattern.MatchString(ticker) || len(name) < 2 || len(name) > 160 || strings.TrimSpace(asset.Exchange) == "" {
		return Target{}, errors.New("selected stock requires an active equity catalog name, ticker and exchange")
	}
	return Target{AssetID: asset.ID, Ticker: ticker, Name: asset.Name, CompanyName: companyName, Exchange: asset.Exchange, OrganizationNames: []string{name}}, nil
}

func TargetQuery(target Target) string {
	// A short ticker such as ON is never searched as a bare common word.
	return `("` + target.OrganizationNames[0] + `" OR "` + target.Ticker + ` stock")`
}

func queryKey(q string) string { sum := sha256.Sum256([]byte(q)); return hex.EncodeToString(sum[:8]) }

func ValidateSelection(req Request) error {
	if len(req.AssetIDs) < 1 || len(req.AssetIDs) > 10 || len(req.Queries) > 0 {
		return errors.New("select 1..10 stocks without custom queries")
	}
	seen := map[int64]bool{}
	for _, id := range req.AssetIDs {
		if id <= 0 || seen[id] {
			return errors.New("invalid or duplicate selected assetId")
		}
		seen[id] = true
	}
	return nil
}

func prepareTargets(req Request) (Request, error) {
	if req.AssetIDs != nil && len(req.AssetIDs) == 0 {
		return req, errors.New("select at least one stock")
	}
	if len(req.AssetIDs) == 0 && len(req.Targets) == 0 {
		return req, nil
	}
	if err := ValidateSelection(req); err != nil {
		return req, err
	}
	if len(req.AssetIDs) != len(req.Targets) {
		return req, errors.New("selected stocks require server catalog resolution")
	}
	for i, t := range req.Targets {
		if t.AssetID != req.AssetIDs[i] || len(t.OrganizationNames) != 1 {
			return req, errors.New("selected catalog snapshot mismatch")
		}
		expected, err := CatalogTarget(domain.AssetOption{ID: t.AssetID, Ticker: t.Ticker, Name: t.Name, CompanyName: t.CompanyName, Exchange: t.Exchange, AssetType: "EQUITY"})
		if err != nil || t.OrganizationNames[0] != expected.OrganizationNames[0] || t.CompanyName != expected.CompanyName {
			return req, errors.New("invalid selected catalog snapshot")
		}
		if req.Provider == "RSS" {
			q := TargetQuery(t)
			req.Queries = append(req.Queries, q)
			req.Targets[i].QueryKey = queryKey(q)
		}
	}
	return req, nil
}
