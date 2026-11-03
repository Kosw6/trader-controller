package domain

import (
	"fmt"
	"math"
	"time"
)

type PriceMoveRule struct {
	RisePercent float64   `json:"risePercent"`
	FallPercent float64   `json:"fallPercent"`
	Version     int64     `json:"version"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

func (r PriceMoveRule) Validate() error {
	if math.IsNaN(r.RisePercent) || math.IsInf(r.RisePercent, 0) || r.RisePercent < 0.01 || r.RisePercent > 10000 || math.IsNaN(r.FallPercent) || math.IsInf(r.FallPercent, 0) || r.FallPercent < 0.01 || r.FallPercent > 100 || r.Version < 1 {
		return fmt.Errorf("상승 기준 0.01~10000%%, 하락 폭 0.01~100%%와 최신 설정 버전이 필요합니다")
	}
	// Match the database precision; do not silently round administrator input.
	for _, v := range []float64{r.RisePercent, r.FallPercent} {
		if math.Abs(v*10000-math.Round(v*10000)) > 0.000001 {
			return fmt.Errorf("기준은 소수점 넷째 자리까지 입력해주세요")
		}
	}
	return nil
}

type PriceMoveFilter struct {
	Date      string
	Start     string
	End       string
	Mode      string
	Query     string
	Direction string
	Offset    int
}

type PriceMove struct {
	AssetID       int64  `json:"assetId"`
	Ticker        string `json:"ticker"`
	Name          string `json:"name"`
	Exchange      string `json:"exchange"`
	Date          string `json:"date"`
	PreviousDate  string `json:"previousDate"`
	Close         string `json:"close"`
	PreviousClose string `json:"previousClose"`
	Percent       string `json:"percent"`
	Currency      string `json:"currency"`
	GapDays       int    `json:"gapDays"`
}

type PriceMoveResult struct {
	Rule           PriceMoveRule `json:"rule"`
	Date           string        `json:"date"`
	Start          string        `json:"start"`
	End            string        `json:"end"`
	Mode           string        `json:"mode"`
	Observations   int64         `json:"observations"`
	Rows           []PriceMove   `json:"rows"`
	Total          int64         `json:"total"`
	Assets         int64         `json:"assets"`
	MissingCurrent int64         `json:"missingCurrent"`
	RejectedPairs  int64         `json:"rejectedPairs"`
	HasMore        bool          `json:"hasMore"`
	CheckedAt      time.Time     `json:"checkedAt"`
}
