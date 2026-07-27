package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kosw006/trader-controller/internal/store"
)

type Store struct {
	pool *pgxpool.Pool
}

// New는 이미 설정된 pgx 연결 풀을 사용하는 PostgreSQL 저장소를 만든다.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Ping은 readiness 검사에서 PostgreSQL 연결 가능 여부를 확인한다.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

type rowScanner interface {
	Scan(dest ...any) error
}

// mapError는 pgx 오류를 HTTP 계층이 이해할 공통 저장소 오류로 변환한다.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return store.ErrConflict
	}
	return err
}
