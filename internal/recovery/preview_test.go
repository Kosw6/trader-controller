package recovery

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/kosw006/trader-controller/internal/domain"
)

func TestAdvice(t *testing.T) {
	now := time.Date(2026, 10, 22, 4, 0, 0, 0, time.UTC)
	failure := func(category string) string {
		return `{"requestPolicy":{"policyVersion":"bounded-v1","failureCategory":"` + category + `","stopReason":"non_retryable","requestAttempts":1,"retriesUsed":0}}`
	}
	tests := []struct{ name, job, status, checkpoint, action, reason string }{
		{"legacy failure", "FAILED", "FAILED", `{}`, "REVIEW", "MISSING_RESULT_CONTRACT"},
		{"legacy skip", "SUCCESS", "SKIPPED", `{}`, "VERIFY_SKIP", "SKIPPED_IS_NOT_VERIFIED_NO_CHANGE"},
		{"raw collected", "SUCCESS", "COLLECTED", `{}`, "VERIFY_RAW_AND_NOTIFICATION", "COLLECTION_IS_NOT_PUBLISHED_DATA"},
		{"success unverified", "SUCCESS", "SUCCESS", `{}`, "VERIFY_RESULT", "INDEPENDENT_RECEIPT_REQUIRED"},
		{"running", "RUNNING", "RUNNING", failure("TRANSIENT_TRANSPORT"), "OBSERVE", "DO_NOT_START_DUPLICATE"},
		{"queued", "RUNNING", "QUEUED", failure("TRANSIENT_TRANSPORT"), "OBSERVE", "DO_NOT_START_DUPLICATE"},
		{"job stop", "CANCELLED", "SUCCESS", failure("TRANSIENT_TRANSPORT"), "PAUSED", "USER_OR_JOB_STOP"},
		{"item stop", "RUNNING", "CANCELLED", failure("TRANSIENT_TRANSPORT"), "PAUSED", "USER_OR_ITEM_STOP"},
		{"explicit stop", "FAILED", "FAILED", failure("USER_PAUSED"), "PAUSED", "USER_STOP_EVIDENCE"},
		{"commit uncertain", "FAILED", "FAILED", failure("COMMIT_UNCERTAIN"), "VERIFY_COMMIT", "INDEPENDENT_READ_BEFORE_ANY_WRITE"},
		{"unproved no change", "SUCCESS", "SKIPPED", failure("NO_CHANGE_VERIFIED"), "VERIFY_NO_CHANGE", "INDEPENDENT_RECEIPT_REQUIRED"},
		{"transient not permission", "FAILED", "FAILED", failure("TRANSIENT_TRANSPORT"), "REVIEW_RETRY_BUDGET", "REQUIRE_PHASE_COMMIT_BUDGET_QUOTA_AND_LEASE"},
		{"rate not permission", "FAILED", "FAILED", failure("RATE_LIMIT"), "REVIEW_RETRY_BUDGET", "REQUIRE_PHASE_COMMIT_BUDGET_QUOTA_AND_LEASE"},
		{"malformed", "FAILED", "FAILED", `{"requestPolicy":`, "REVIEW", "INVALID_CHECKPOINT"},
		{"null evidence", "FAILED", "FAILED", `{"requestPolicy":null}`, "REVIEW", "INVALID_POLICY_EVIDENCE"},
		{"unsupported", "FAILED", "FAILED", `{"requestPolicy":{"policyVersion":"v2"}}`, "REVIEW", "UNSUPPORTED_POLICY_VERSION"},
		{"conflicting version", "FAILED", "FAILED", `{"requestPolicy":{"version":"v2","policyVersion":"bounded-v1"}}`, "REVIEW", "UNSUPPORTED_POLICY_VERSION"},
		{"unknown category", "FAILED", "FAILED", failure("secret-error-message"), "REVIEW", "UNKNOWN_CATEGORY"},
		{"empty despite collection", "SUCCESS", "COLLECTED", `{"requestPolicy":{"version":"bounded-v1","result":{"policyVersion":"bounded-v1","failureCategory":"EMPTY_UNRESOLVED"}}}`, "REVIEW", "NON_AUTOMATIC_FAILURE"},
		{"http not data", "SUCCESS", "COLLECTED", `{"requestPolicy":{"version":"bounded-v1","attempts":[{"outcome":"HTTP_OK"}],"result":null}}`, "VERIFY_RAW_AND_NOTIFICATION", "COLLECTION_IS_NOT_PUBLISHED_DATA"},
		{"nested version", "FAILED", "FAILED", `{"requestPolicy":{"version":"bounded-v1","result":{"failureCategory":"RATE_LIMIT"}}}`, "REVIEW", "AMBIGUOUS_POLICY_EVIDENCE"},
	}
	for _, category := range []string{"AUTH_BLOCKED", "EMPTY_UNRESOLVED", "SOURCE_NOT_FOUND", "QUALITY_OR_SEMANTIC_CONFLICT", "IMPLEMENTATION_ERROR", "RESOURCE_BLOCKED", "UNCLASSIFIED"} {
		tests = append(tests, struct{ name, job, status, checkpoint, action, reason string }{category, "FAILED", "FAILED", failure(category), "REVIEW", "NON_AUTOMATIC_FAILURE"})
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := advise(tc.job, domain.JobItem{ID: 1, Status: tc.status, Checkpoint: json.RawMessage(tc.checkpoint)}, now)
			if a.Action != tc.action || a.Reason != tc.reason || a.ExecutionAllowed {
				t.Fatalf("unexpected advice: %+v", a)
			}
		})
	}
}

func TestCooldownNeverAuthorizesOrShortens(t *testing.T) {
	now := time.Date(2026, 10, 22, 4, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, extra, action string
		completed           *time.Time
	}{
		{"server minimum", `,"retryAfterSeconds":120`, "WAIT", &now},
		{"fraction", `,"retryAfterSeconds":0.25`, "WAIT", &now},
		{"long", `,"retryAfterSeconds":86400`, "WAIT", &now},
		{"expired not reset", `,"retryAfterSeconds":0`, "REVIEW_RETRY_BUDGET", &now},
		{"negative", `,"retryAfterSeconds":-1`, "REVIEW", &now},
		{"overflow", `,"retryAfterSeconds":1e100`, "REVIEW", &now},
		{"no timestamp", `,"retryAfterSeconds":120`, "REVIEW", nil},
		{"invalid header", `,"invalidRetryAfter":true`, "REVIEW", &now},
		{"wrong type", `,"retryAfterSeconds":"120"`, "REVIEW", &now},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item := domain.JobItem{Status: "FAILED", CompletedAt: tc.completed, Checkpoint: json.RawMessage(`{"requestPolicy":{"policyVersion":"bounded-v1","failureCategory":"RATE_LIMIT","stopReason":"retry_budget"` + tc.extra + `}}`)}
			a := advise("FAILED", item, now)
			if a.Action != tc.action || a.ExecutionAllowed {
				t.Fatalf("advice: %+v", a)
			}
			if tc.name == "server minimum" && !a.NotBefore.Equal(now.Add(120*time.Second)) {
				t.Fatal("server wait shortened")
			}
			if tc.name == "fraction" && !a.NotBefore.Equal(now.Add(250*time.Millisecond)) {
				t.Fatal("fractional wait truncated")
			}
		})
	}
}

func TestPreviewCountsOnlyObservedRowsAndRedacts(t *testing.T) {
	secret := "do-not-return-error-or-storage-location"
	p := Build(domain.JobDetail{Job: domain.Job{ID: 9, Status: "FAILED", ErrorMessage: &secret}, Items: []domain.JobItem{
		{ID: 1, Status: "FAILED", ItemKey: secret, ErrorMessage: &secret, Checkpoint: json.RawMessage(`{"storage_key":"` + secret + `"}`)},
		{ID: 2, Status: "COLLECTED"}, {ID: 3, Status: "SKIPPED"},
	}}, time.Now())
	count := 0
	for _, n := range p.ActionCounts {
		count += n
	}
	if count != 3 || p.ExecutionAllowed || p.CompletionVerified || !strings.Contains(p.Scope, "NON_ATOMIC") {
		t.Fatalf("bad summary %+v", p)
	}
	body, _ := json.Marshal(p)
	if strings.Contains(string(body), secret) {
		t.Fatal("unapproved metadata exposed")
	}
}
