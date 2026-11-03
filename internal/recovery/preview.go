// Package recovery translates collector evidence into read-only recovery advice.
// Advice is not a lease, retry authorization, or proof of a committed result.
package recovery

import (
	"encoding/json"
	"math"
	"time"

	"github.com/kosw006/trader-controller/internal/domain"
)

type ItemAdvice struct {
	ItemID           int64      `json:"itemId"`
	Action           string     `json:"action"`
	Reason           string     `json:"reason"`
	Category         string     `json:"category,omitempty"`
	NotBefore        *time.Time `json:"notBefore,omitempty"`
	ExecutionAllowed bool       `json:"executionAllowed"`
}

type Preview struct {
	Version            string         `json:"version"`
	JobID              int64          `json:"jobId"`
	ObservedAt         time.Time      `json:"observedAt"`
	Scope              string         `json:"scope"`
	CompletionVerified bool           `json:"completionVerified"`
	ExecutionAllowed   bool           `json:"executionAllowed"`
	Items              []ItemAdvice   `json:"items"`
	ActionCounts       map[string]int `json:"actionCounts"`
}

// Both shapes are emitted by bounded-v1: direct failure and successful HTTP
// collection with an optional unresolved result. HTTP_OK is not data validation.
type policyEvidence struct {
	Version           string          `json:"version"`
	PolicyVersion     string          `json:"policyVersion"`
	Category          string          `json:"failureCategory"`
	StopReason        string          `json:"stopReason"`
	RetryAfter        *float64        `json:"retryAfterSeconds"`
	InvalidRetryAfter bool            `json:"invalidRetryAfter"`
	Result            *policyEvidence `json:"result"`
}

var categories = map[string]bool{
	"TRANSIENT_TRANSPORT": true, "RATE_LIMIT": true, "AUTH_BLOCKED": true,
	"EMPTY_UNRESOLVED": true, "NO_CHANGE_VERIFIED": true, "SOURCE_NOT_FOUND": true,
	"QUALITY_OR_SEMANTIC_CONFLICT": true, "COMMIT_UNCERTAIN": true,
	"IMPLEMENTATION_ERROR": true, "RESOURCE_BLOCKED": true,
	"USER_PAUSED": true, "UNCLASSIFIED": true,
}

// Build deliberately scopes counts to the observed detail rows. GetJob currently
// reads job/items separately and caps items at 200; this is NOT a completion audit.
func Build(detail domain.JobDetail, now time.Time) Preview {
	p := Preview{Version: "recovery-preview-v1", JobID: detail.Job.ID,
		ObservedAt: now.UTC(), Scope: "OBSERVED_DETAIL_ROWS_NON_ATOMIC_MAX_200",
		Items: make([]ItemAdvice, 0, len(detail.Items)), ActionCounts: map[string]int{}}
	for _, item := range detail.Items {
		a := advise(detail.Job.Status, item, now)
		p.Items = append(p.Items, a)
		p.ActionCounts[a.Action]++
	}
	return p
}

func advise(jobStatus string, item domain.JobItem, now time.Time) ItemAdvice {
	a := ItemAdvice{ItemID: item.ID, Action: "REVIEW", Reason: "MISSING_RESULT_CONTRACT"}
	set := func(action, reason string) ItemAdvice { a.Action, a.Reason = action, reason; return a }
	// Explicit user stop wins over stale success/failure metadata. No next attempt.
	switch jobStatus {
	case "CANCELLED", "CANCEL_REQUESTED", "PAUSED", "PAUSED_USER":
		return set("PAUSED", "USER_OR_JOB_STOP")
	}
	switch item.Status {
	case "CANCELLED", "PAUSED", "PAUSED_USER":
		return set("PAUSED", "USER_OR_ITEM_STOP")
	case "RUNNING", "QUEUED", "SCHEDULED":
		return set("OBSERVE", "DO_NOT_START_DUPLICATE")
	}
	var checkpoint map[string]json.RawMessage
	if len(item.Checkpoint) > 0 && json.Unmarshal(item.Checkpoint, &checkpoint) != nil {
		return set("REVIEW", "INVALID_CHECKPOINT")
	}
	if raw, exists := checkpoint["requestPolicy"]; exists {
		var evidence *policyEvidence
		if json.Unmarshal(raw, &evidence) != nil || evidence == nil {
			return set("REVIEW", "INVALID_POLICY_EVIDENCE")
		}
		if (evidence.Version == "" && evidence.PolicyVersion == "") ||
			(evidence.Version != "" && evidence.Version != "bounded-v1") ||
			(evidence.PolicyVersion != "" && evidence.PolicyVersion != "bounded-v1") {
			return set("REVIEW", "UNSUPPORTED_POLICY_VERSION")
		}
		if evidence.Result != nil {
			if evidence.Category != "" || evidence.Result.PolicyVersion != "bounded-v1" || evidence.Result.Result != nil {
				return set("REVIEW", "AMBIGUOUS_POLICY_EVIDENCE")
			}
			evidence = evidence.Result
		}
		if evidence.Category != "" {
			if !categories[evidence.Category] {
				return set("REVIEW", "UNKNOWN_CATEGORY")
			}
			a.Category = evidence.Category
			switch evidence.Category {
			case "USER_PAUSED":
				return set("PAUSED", "USER_STOP_EVIDENCE")
			case "COMMIT_UNCERTAIN":
				return set("VERIFY_COMMIT", "INDEPENDENT_READ_BEFORE_ANY_WRITE")
			case "NO_CHANGE_VERIFIED":
				return set("VERIFY_NO_CHANGE", "INDEPENDENT_RECEIPT_REQUIRED")
			case "TRANSIENT_TRANSPORT", "RATE_LIMIT":
				if evidence.InvalidRetryAfter {
					return set("REVIEW", "INVALID_RETRY_AFTER")
				}
				// Never clamp a long server wait into an earlier retry, including overflow.
				if evidence.RetryAfter != nil {
					seconds := *evidence.RetryAfter
					if math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 0 || seconds > 315360000 || item.CompletedAt == nil {
						return set("REVIEW", "UNUSABLE_COOLDOWN_EVIDENCE")
					}
					due := item.CompletedAt.Add(time.Duration(math.Ceil(seconds * 1e9))).UTC()
					a.NotBefore = &due
					if now.Before(due) {
						return set("WAIT", "PROVIDER_COOLDOWN_MINIMUM")
					}
				}
				// Local retriesUsed is not a persisted cross-worker budget. Exhaustion
				// must not be reset just by creating a fresh Job/worker process.
				return set("REVIEW_RETRY_BUDGET", "REQUIRE_PHASE_COMMIT_BUDGET_QUOTA_AND_LEASE")
			default:
				return set("REVIEW", "NON_AUTOMATIC_FAILURE")
			}
		}
	}
	switch item.Status {
	case "COLLECTED":
		return set("VERIFY_RAW_AND_NOTIFICATION", "COLLECTION_IS_NOT_PUBLISHED_DATA")
	case "SUCCESS":
		return set("VERIFY_RESULT", "INDEPENDENT_RECEIPT_REQUIRED")
	case "SKIPPED":
		return set("VERIFY_SKIP", "SKIPPED_IS_NOT_VERIFIED_NO_CHANGE")
	default:
		return a
	}
}
