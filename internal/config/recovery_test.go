package config

import "testing"

func TestRecoveryCoordinatorOptIn(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://unused/test")
	t.Setenv("ADMIN_API_KEY", "test-only")
	for _, tc := range []struct {
		value   string
		enabled bool
	}{{"", false}, {"false", false}, {"invalid", false}, {"true", true}} {
		t.Setenv("RECOVERY_COORDINATOR_ENABLED", tc.value)
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.RecoveryCoordinatorEnabled != tc.enabled {
			t.Fatalf("flag %q", tc.value)
		}
	}
}
