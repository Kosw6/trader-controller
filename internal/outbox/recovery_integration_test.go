package outbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kosw006/trader-controller/internal/domain"
)

type recoveryPublisher struct {
	unavailable, fail bool
	calls             int
}

func (p *recoveryPublisher) Health(context.Context) error {
	if p.unavailable {
		return errors.New("offline")
	}
	return nil
}
func (p *recoveryPublisher) Publish(context.Context, domain.OutboxEvent) (PublishResult, error) {
	p.calls++
	if p.fail {
		return PublishResult{}, errors.New("test failure")
	}
	return PublishResult{1, 42}, nil
}
func (p *recoveryPublisher) Close() error { return nil }

func TestCoordinatedOutboxSurvivesBrokerFailure(t *testing.T) {
	dsn := os.Getenv("RECOVERY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated DB not configured")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.Host != "127.0.0.1" || cfg.ConnConfig.Port != 55451 || cfg.ConnConfig.Database != "recovery451_test" {
		t.Fatal("not isolated DB")
	}
	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema := fmt.Sprintf("outbox_%d", time.Now().UnixNano())
	if _, err = pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	isolated, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer isolated.Close()
	_, err = isolated.Exec(ctx, `CREATE TABLE pipeline_outbox(id bigserial PRIMARY KEY,source_object_id bigint,job_item_id bigint,event_type text,topic text,message_key text,
 idempotency_key text UNIQUE,payload jsonb,status text,attempt_count integer,next_attempt_at timestamptz,published_at timestamptz,last_error text,
 kafka_partition integer,kafka_offset bigint,created_at timestamptz DEFAULT now(),updated_at timestamptz DEFAULT now());
 INSERT INTO pipeline_outbox(source_object_id,job_item_id,event_type,topic,message_key,idempotency_key,payload,status,attempt_count,next_attempt_at)
 VALUES(1,1,'COORDINATED_RAW_READY','trader.raw.coordinated.ready','work','COORDINATED_RAW_READY:work','{}','PENDING',0,now());`)
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recoveryPublisher{unavailable: true}
	relay := NewRelay(NewRepository(isolated), publisher, Config{BatchSize: 1, StaleAfter: time.Hour, MaxAttempts: 2, RetryBaseDelay: 0, RetryMaxDelay: time.Second}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	check := func(want string, n int) {
		t.Helper()
		var state string
		var attempts int
		if e := isolated.QueryRow(ctx, "SELECT status,attempt_count FROM pipeline_outbox WHERE id=1").Scan(&state, &attempts); e != nil {
			t.Fatal(e)
		}
		if state != want || attempts != n {
			t.Fatalf("got %s/%d want %s/%d", state, attempts, want, n)
		}
	}
	if err = relay.runOnce(ctx); err != nil {
		t.Fatal(err)
	}
	check("PENDING", 0)
	publisher.unavailable = false
	publisher.fail = true
	if err = relay.runOnce(ctx); err != nil {
		t.Fatal(err)
	}
	check("RETRY_WAIT", 1)
	publisher.fail = false
	if err = relay.runOnce(ctx); err != nil {
		t.Fatal(err)
	}
	check("PUBLISHED", 2)
	if err = relay.runOnce(ctx); err != nil {
		t.Fatal(err)
	}
	check("PUBLISHED", 2)
	if publisher.calls != 2 {
		t.Fatal("unexpected publish attempts")
	}
}
