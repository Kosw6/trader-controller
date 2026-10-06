package postgres

import (
	"context"
	"encoding/json"

	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/store"
)

// strpos는 %, _를 와일드카드로 해석하지 않는다. 페이지마다 종목 단위로 잘라 조회한다.
func (s *Store) ListAssetAliases(ctx context.Context, f domain.AssetLinkFilter) ([]domain.AssetAliases, error) {
	rows, err := s.pool.Query(ctx, `SELECT a.id,a.ticker,a.name,a.exchange,a.active,
 COALESCE((SELECT jsonb_agg(jsonb_build_object('id',aa.id::text,'alias',aa.alias,
 'aliasType',aa.alias_type,'source',aa.source) ORDER BY aa.id) FROM asset_alias aa WHERE aa.asset_id=a.id),'[]'::jsonb)
 FROM asset a WHERE ($1='' OR strpos(lower(a.ticker),lower($1))>0 OR strpos(lower(a.name),lower($1))>0
 OR EXISTS(SELECT 1 FROM asset_alias aa WHERE aa.asset_id=a.id AND strpos(lower(aa.alias),lower($1))>0))
 ORDER BY a.ticker,a.exchange,a.id LIMIT $2 OFFSET $3`, f.Query, f.Limit, f.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []domain.AssetAliases{}
	for rows.Next() {
		var item domain.AssetAliases
		var aliases []byte
		if err := rows.Scan(&item.ID, &item.Ticker, &item.Name, &item.Exchange, &item.Active, &aliases); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(aliases, &item.Aliases); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

// 종목 행 잠금으로 같은 종목의 별칭 편집을 직렬화한다. 다른 종목의 같은 별칭은
// 허용하며 AI는 그런 이름을 자동 확정하지 않고 후보 선택을 요청한다.
func (s *Store) SaveAssetAlias(ctx context.Context, assetID, aliasID int64, value, normalized string) (domain.AssetAlias, error) {
	var result domain.AssetAlias
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	var id int64
	if err = tx.QueryRow(ctx, `SELECT id FROM asset WHERE id=$1 FOR UPDATE`, assetID).Scan(&id); err != nil {
		return result, mapError(err)
	}
	var duplicate bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM asset_alias WHERE asset_id=$1 AND id<>$2 AND (lower(alias)=lower($3) OR normalized_alias=$4))`, assetID, aliasID, value, normalized).Scan(&duplicate)
	if err != nil {
		return result, err
	}
	if duplicate {
		return result, store.ErrConflict
	}
	if aliasID == 0 {
		err = tx.QueryRow(ctx, `INSERT INTO asset_alias(asset_id,alias,normalized_alias,alias_type,source) VALUES($1,$2,$3,'MANUAL','ADMIN') RETURNING id,alias,alias_type,source`, assetID, value, normalized).Scan(&result.ID, &result.Alias, &result.AliasType, &result.Source)
	} else {
		err = tx.QueryRow(ctx, `UPDATE asset_alias SET alias=$3,normalized_alias=$4,source='ADMIN' WHERE asset_id=$1 AND id=$2 RETURNING id,alias,alias_type,source`, assetID, aliasID, value, normalized).Scan(&result.ID, &result.Alias, &result.AliasType, &result.Source)
	}
	if err != nil {
		return result, mapError(err)
	}
	return result, mapError(tx.Commit(ctx))
}

func (s *Store) DeleteAssetAlias(ctx context.Context, assetID, aliasID int64) error {
	result, err := s.pool.Exec(ctx, `DELETE FROM asset_alias WHERE asset_id=$1 AND id=$2`, assetID, aliasID)
	if err != nil {
		return mapError(err)
	}
	if result.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}
