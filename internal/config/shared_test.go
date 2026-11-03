package config

import "testing"

func TestSharedHostingFlagFailsClosed(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://unused/test")
	t.Setenv("ADMIN_API_KEY", "test-only")
	t.Setenv("WORKER_SHARED_HOST", "tru")
	if _, err := Load(); err == nil {
		t.Fatal("invalid shared setting was accepted")
	}
	t.Setenv("WORKER_SHARED_HOST", "true")
	cfg, err := Load()
	if err != nil || !cfg.SharedWorkerHost {
		t.Fatal("shared setting missing")
	}
}
