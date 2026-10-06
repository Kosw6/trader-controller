package domain

// 별칭은 종목 식별 보조 정보다. 종목명/티커 원본을 이 API로 변경하지 않는다.
type AssetAlias struct {
	ID        int64  `json:"id,string"`
	Alias     string `json:"alias"`
	AliasType string `json:"aliasType"`
	Source    string `json:"source"`
}

type AssetAliases struct {
	ID       int64        `json:"id,string"`
	Ticker   string       `json:"ticker"`
	Name     string       `json:"name"`
	Exchange string       `json:"exchange"`
	Active   bool         `json:"active"`
	Aliases  []AssetAlias `json:"aliases"`
}

type SaveAssetAlias struct {
	Alias string `json:"alias"`
}
