package postgres

import (
	"context"
	"time"

	"github.com/kosw006/trader-controller/internal/domain"
)

const adminSessionColumns = `
    id,
    session_token_hash,
    actor,
    ip_address::text,
    user_agent,
    created_at,
    last_seen_at,
    expires_at,
    revoked_at
`

func scanAdminSession(row rowScanner) (domain.AdminSession, error) {
	var value domain.AdminSession
	err := row.Scan(
		&value.ID,
		&value.SessionTokenHash,
		&value.Actor,
		&value.IPAddress,
		&value.UserAgent,
		&value.CreatedAt,
		&value.LastSeenAt,
		&value.ExpiresAt,
		&value.RevokedAt,
	)
	if err != nil {
		return domain.AdminSession{}, mapError(err)
	}
	return value, nil
}

// CreateAdminSession stores only the hash of the browser session token.
func (s *Store) CreateAdminSession(ctx context.Context, req domain.CreateAdminSessionRequest) (domain.AdminSession, error) {
	value, err := scanAdminSession(s.pool.QueryRow(ctx, `
        INSERT INTO admin_session (
            session_token_hash,
            actor,
            ip_address,
            user_agent,
            expires_at
        )
        VALUES ($1, $2, $3::inet, $4, $5)
        RETURNING `+adminSessionColumns,
		req.SessionTokenHash,
		req.Actor,
		req.IPAddress,
		req.UserAgent,
		req.ExpiresAt,
	))
	if err != nil {
		return domain.AdminSession{}, err
	}
	return value, nil
}

// GetActiveAdminSession returns a non-expired, non-revoked session by token hash.
func (s *Store) GetActiveAdminSession(ctx context.Context, tokenHash string, now time.Time) (domain.AdminSession, error) {
	value, err := scanAdminSession(s.pool.QueryRow(ctx, `
        SELECT `+adminSessionColumns+`
        FROM admin_session
        WHERE session_token_hash = $1
          AND revoked_at IS NULL
          AND expires_at > $2
        LIMIT 1
    `, tokenHash, now))
	if err != nil {
		return domain.AdminSession{}, err
	}
	return value, nil
}

// TouchAdminSession records that a valid session was used recently.
func (s *Store) TouchAdminSession(ctx context.Context, id int64, seenAt time.Time) error {
	_, err := s.pool.Exec(ctx, `
        UPDATE admin_session
        SET last_seen_at = $2
        WHERE id = $1
    `, id, seenAt)
	return mapError(err)
}

// RevokeAdminSession invalidates a browser admin session.
func (s *Store) RevokeAdminSession(ctx context.Context, tokenHash string, revokedAt time.Time) error {
	_, err := s.pool.Exec(ctx, `
        UPDATE admin_session
        SET revoked_at = $2
        WHERE session_token_hash = $1
          AND revoked_at IS NULL
    `, tokenHash, revokedAt)
	return mapError(err)
}
