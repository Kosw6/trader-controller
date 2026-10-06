package app

import (
	"errors"
	"github.com/kosw006/trader-controller/internal/store"
	"strings"
	"testing"
)

func TestNormalizeAlias(t *testing.T) {
	value, normalized, err := normalizeAlias("  엔비 NVDA.A  ")
	if err != nil || value != "엔비 NVDA.A" || normalized != "엔비nvdaa" {
		t.Fatalf("unexpected %q %q %v", value, normalized, err)
	}
	for _, raw := range []string{"", "   ", "---", "엔비\n디아", strings.Repeat("가", 256)} {
		if _, _, err := normalizeAlias(raw); !errors.Is(err, store.ErrValidation) {
			t.Errorf("accepted %q", raw)
		}
	}
}
