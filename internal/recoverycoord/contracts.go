// Package recoverycoord coordinates opt-in workers; it never starts a worker.
package recoverycoord

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"time"
)

var (
	ErrInvalid   = errors.New("invalid recovery contract")
	ErrConflict  = errors.New("immutable recovery contract conflict")
	ErrLease     = errors.New("lease missing, expired or fenced")
	ErrBudget    = errors.New("persistent budget exhausted")
	ErrCooling   = errors.New("shared provider cooldown or in-flight request")
	ErrReview    = errors.New("manual review or explicit resume required")
	ErrReserved  = errors.New("request already reserved; inspect receipt, do not send again")
	ErrReceipt   = errors.New("result already recorded; independently verify receipt")
	ErrUncertain = errors.New("commit outcome uncertain; read ledger before any retry")
)

type Identity struct {
	Provider   string `json:"provider"`
	QuotaScope string `json:"quotaScope"` // Non-secret credential quota alias, not the credential.
	Target     string `json:"target"`     // Stable asset/company identifier, not only a ticker.
	Dataset    string `json:"dataset"`
	Start      string `json:"start"`
	End        string `json:"end"`
	Revision   string `json:"revision"` // Approved manifest/content generation, not a Job/attempt ID.
}
type Policy struct {
	Version      string `json:"version"`
	MaxClaims    int64  `json:"maxClaims"`
	MaxRequests  int64  `json:"maxRequests"`
	MaxRetries   int64  `json:"maxRetries"`
	MaxElapsedMS int64  `json:"maxElapsedMs"`
	LeaseMS      int64  `json:"leaseMs"`
	IntervalMS   int64  `json:"intervalMs"`
	BaseDelayMS  int64  `json:"baseDelayMs"`
	MaxDelayMS   int64  `json:"maxDelayMs"`
	RateLimitMS  int64  `json:"rateLimitMs"`
}
type Lease struct {
	WorkKey string `json:"workKey"`
	Owner   string `json:"owner"`
	Fence   int64  `json:"fence"`
}
type Work struct {
	ServerNow   time.Time  `json:"serverNow"`
	Key         string     `json:"key"`
	Identity    Identity   `json:"identity"`
	Policy      Policy     `json:"policy"`
	Scope       string     `json:"scope"`
	Phase       string     `json:"phase"`
	State       string     `json:"state"`
	Fence       int64      `json:"fence"`
	Owner       string     `json:"owner"`
	LeaseUntil  *time.Time `json:"leaseUntil"`
	Deadline    time.Time  `json:"deadline"`
	Claims      int64      `json:"claims"`
	Requests    int64      `json:"requests"`
	Retries     int64      `json:"retries"`
	HeartbeatAt *time.Time `json:"heartbeatAt"`
	ProgressAt  time.Time  `json:"progressAt"`
}
type Receipt struct {
	ID    string `json:"id"`
	From  string `json:"from"`
	To    string `json:"to"`
	Hash  string `json:"hash"`
	Fence int64  `json:"fence"`
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9_.:/-]{1,160}$`)
var digest = regexp.MustCompile(`^[0-9a-f]{64}$`)
var assetID = regexp.MustCompile(`^asset:[1-9][0-9]*$`)
var companyID = regexp.MustCompile(`^company:[1-9][0-9]*$`)
var newsID = regexp.MustCompile(`^news:[0-9a-f]{64}$`)

func hash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (i Identity) Key() string   { return hash(i) }
func (i Identity) Scope() string { return hash([]string{i.Provider, i.QuotaScope}) }
func (i Identity) validate() error {
	for _, s := range []string{i.Provider, i.QuotaScope, i.Target, i.Dataset, i.Revision} {
		if !identifier.MatchString(s) {
			return ErrInvalid
		}
	}
	start, e1 := time.Parse("2006-01-02", i.Start)
	if (i.Provider != "KIS" && i.Provider != "SEC" && i.Provider != "NEWS") || !digest.MatchString(i.Revision) {
		return ErrInvalid
	}
	if (i.Provider == "KIS" && !assetID.MatchString(i.Target)) || (i.Provider == "SEC" && !companyID.MatchString(i.Target)) {
		return ErrInvalid
	}
	if i.Provider == "NEWS" && (!newsID.MatchString(i.Target) || (i.Dataset != "news-rss" && i.Dataset != "news-gdelt") || i.Start != i.End) {
		return ErrInvalid
	}
	end, e2 := time.Parse("2006-01-02", i.End)
	if e1 != nil || e2 != nil || end.Before(start) {
		return ErrInvalid
	}
	return nil
}
func (p Policy) validate() error {
	if p.Version != "coord-v1" || p.MaxClaims < 1 || p.MaxClaims > 10000 || p.MaxRequests < 1 || p.MaxRequests > 1000000 || p.MaxRetries < 0 || p.MaxRetries > p.MaxRequests || p.LeaseMS < 100 || p.LeaseMS > 300000 || p.MaxElapsedMS < p.LeaseMS || p.MaxElapsedMS > 604800000 || p.IntervalMS < 0 || p.IntervalMS > 60000 || p.BaseDelayMS < 1 || p.MaxDelayMS < p.BaseDelayMS || p.MaxDelayMS > 86400000 || p.RateLimitMS < 1 || p.RateLimitMS > 86400000 {
		return ErrInvalid
	}
	return nil
}
func validLease(l Lease) bool {
	return digest.MatchString(l.WorkKey) && identifier.MatchString(l.Owner) && l.Fence > 0
}
func nextPhase(from, to string) bool {
	return map[string]string{"FETCH_RAW": "RAW_READY", "RAW_READY": "ETL_READY", "ETL_READY": "CANDIDATE", "CANDIDATE": "VERIFIED", "VERIFIED": "COMPLETE"}[from] == to && to != ""
}
