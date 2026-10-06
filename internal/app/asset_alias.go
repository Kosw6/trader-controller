package app

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/kosw006/trader-controller/internal/domain"
	"github.com/kosw006/trader-controller/internal/store"
)

var aliasCharacters = regexp.MustCompile(`[^0-9a-z가-힣]+`)

// 수집기와 동일한 정규화 값을 보관하되 실제 식별은 원래 별칭의 정확 일치다.
func normalizeAlias(raw string) (string, string, error) {
	value := strings.TrimSpace(raw)
	if value == "" || utf8.RuneCountInString(value) > 255 || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return "", "", fmt.Errorf("%w: 별칭은 제어문자 없이 1~255자로 입력해주세요", store.ErrValidation)
	}
	normalized := aliasCharacters.ReplaceAllString(strings.ToLower(value), "")
	if normalized == "" {
		return "", "", fmt.Errorf("%w: 별칭에 한글·영문·숫자가 필요합니다", store.ErrValidation)
	}
	return value, normalized, nil
}

func (s *Service) ListAssetAliases(ctx context.Context, f domain.AssetLinkFilter) ([]domain.AssetAliases, error) {
	f.Query = strings.TrimSpace(f.Query)
	if utf8.RuneCountInString(f.Query) > 255 {
		return nil, fmt.Errorf("%w: 검색어는 255자 이하로 입력해주세요", store.ErrValidation)
	}
	if f.Limit <= 0 || f.Limit > 50 {
		f.Limit = 25
	}
	if f.Offset < 0 {
		f.Offset = 0
	}
	return s.store.ListAssetAliases(ctx, f)
}

func (s *Service) SaveAssetAlias(ctx context.Context, assetID, aliasID int64, req domain.SaveAssetAlias) (domain.AssetAlias, error) {
	value, normalized, err := normalizeAlias(req.Alias)
	if err != nil {
		return domain.AssetAlias{}, err
	}
	return s.store.SaveAssetAlias(ctx, assetID, aliasID, value, normalized)
}

func (s *Service) DeleteAssetAlias(ctx context.Context, assetID, aliasID int64) error {
	return s.store.DeleteAssetAlias(ctx, assetID, aliasID)
}
