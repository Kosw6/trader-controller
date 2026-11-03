package recoverycoord

import (
	"strings"
	"testing"
)

func TestNewsIdentityBindsOneCollectionUnit(t *testing.T) {
	i := Identity{Provider: "NEWS", QuotaScope: "news-rss-production", Target: "news:" + strings.Repeat("a", 64), Dataset: "news-rss", Start: "2026-11-01", End: "2026-11-01", Revision: strings.Repeat("b", 64)}
	if i.validate() != nil {
		t.Fatal("valid NEWS unit rejected")
	}
	for _, change := range []func(*Identity){func(x *Identity) { x.Target = "asset:1" }, func(x *Identity) { x.Dataset = "arbitrary" }, func(x *Identity) { x.End = "2026-11-02" }} {
		invalid := i
		change(&invalid)
		if invalid.validate() != ErrInvalid {
			t.Fatal("unbound news identity accepted")
		}
	}
}
