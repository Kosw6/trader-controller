package domain

import (
	"encoding/json"
	"time"
)

type Schedule struct {
	ID             int64           `json:"id"`
	JobType        string          `json:"jobType"`
	CronExpression string          `json:"cronExpression"`
	Timezone       string          `json:"timezone"`
	Enabled        bool            `json:"enabled"`
	Parameters     json.RawMessage `json:"parameters"`
	LastRunAt      *time.Time      `json:"lastRunAt,omitempty"`
	NextRunAt      *time.Time      `json:"nextRunAt,omitempty"`
	CreatedAt      time.Time       `json:"createdAt"`
	UpdatedAt      time.Time       `json:"updatedAt"`
}

type Job struct {
	ID             int64           `json:"id"`
	ScheduleID     *int64          `json:"scheduleId,omitempty"`
	JobType        string          `json:"jobType"`
	Source         string          `json:"source"`
	Status         string          `json:"status"`
	Priority       int             `json:"priority"`
	ScheduledAt    *time.Time      `json:"scheduledAt,omitempty"`
	StartedAt      *time.Time      `json:"startedAt,omitempty"`
	CompletedAt    *time.Time      `json:"completedAt,omitempty"`
	RequestedBy    string          `json:"requestedBy"`
	IdempotencyKey string          `json:"idempotencyKey"`
	RetryCount     int             `json:"retryCount"`
	Parameters     json.RawMessage `json:"parameters"`
	ErrorCode      *string         `json:"errorCode,omitempty"`
	ErrorMessage   *string         `json:"errorMessage,omitempty"`
	CreatedAt      time.Time       `json:"createdAt"`
	UpdatedAt      time.Time       `json:"updatedAt"`
}

type JobItem struct {
	ID           int64           `json:"id"`
	JobID        int64           `json:"jobId"`
	ItemKey      string          `json:"itemKey"`
	Status       string          `json:"status"`
	AttemptCount int             `json:"attemptCount"`
	Checkpoint   json.RawMessage `json:"checkpoint,omitempty"`
	ErrorCode    *string         `json:"errorCode,omitempty"`
	ErrorMessage *string         `json:"errorMessage,omitempty"`
	StartedAt    *time.Time      `json:"startedAt,omitempty"`
	CompletedAt  *time.Time      `json:"completedAt,omitempty"`
}

type JobDetail struct {
	Job   Job       `json:"job"`
	Items []JobItem `json:"items"`
}

type OutboxEvent struct {
	ID             int64           `json:"id"`
	SourceObjectID *int64          `json:"sourceObjectId,omitempty"`
	JobItemID      int64           `json:"jobItemId"`
	EventType      string          `json:"eventType"`
	Topic          string          `json:"topic"`
	MessageKey     string          `json:"messageKey"`
	IdempotencyKey string          `json:"idempotencyKey"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	AttemptCount   int             `json:"attemptCount"`
	NextAttemptAt  *time.Time      `json:"nextAttemptAt,omitempty"`
	PublishedAt    *time.Time      `json:"publishedAt,omitempty"`
	LastError      *string         `json:"lastError,omitempty"`
	KafkaPartition *int            `json:"kafkaPartition,omitempty"`
	KafkaOffset    *int64          `json:"kafkaOffset,omitempty"`
	CreatedAt      time.Time       `json:"createdAt"`
	UpdatedAt      time.Time       `json:"updatedAt"`
}

type ProcessedEvent struct {
	ConsumerName   string    `json:"consumerName"`
	IdempotencyKey string    `json:"idempotencyKey"`
	Topic          string    `json:"topic"`
	PartitionNo    *int      `json:"partitionNo,omitempty"`
	OffsetNo       *int64    `json:"offsetNo,omitempty"`
	Status         string    `json:"status"`
	ErrorMessage   *string   `json:"errorMessage,omitempty"`
	ProcessedAt    time.Time `json:"processedAt"`
}

type SourceObjectStatus struct {
	ID                     int64      `json:"id"`
	Provider               string     `json:"provider"`
	DataType               string     `json:"dataType"`
	ExternalID             *string    `json:"externalId,omitempty"`
	PipelineJobItemID      *int64     `json:"pipelineJobItemId,omitempty"`
	JobID                  *int64     `json:"jobId,omitempty"`
	ItemKey                *string    `json:"itemKey,omitempty"`
	JobType                *string    `json:"jobType,omitempty"`
	StorageBackend         string     `json:"storageBackend"`
	StorageKey             string     `json:"storageKey"`
	ProcessingStatus       string     `json:"processingStatus"`
	OperationalStatus      string     `json:"operationalStatus"`
	ProcessingStartedAt    *time.Time `json:"processingStartedAt,omitempty"`
	ProcessingFinishedAt   *time.Time `json:"processingFinishedAt,omitempty"`
	ProcessingErrorMessage *string    `json:"processingErrorMessage,omitempty"`
	ProcessingAttemptCount int        `json:"processingAttemptCount"`
	LineageCount           int64      `json:"lineageCount"`
	TargetTables           []string   `json:"targetTables"`
	CollectedAt            time.Time  `json:"collectedAt"`
	LastProcessedAt        *time.Time `json:"lastProcessedAt,omitempty"`
	OutboxID               *int64     `json:"outboxId,omitempty"`
	OutboxStatus           *string    `json:"outboxStatus,omitempty"`
	OutboxTopic            *string    `json:"outboxTopic,omitempty"`
	OutboxKafkaPartition   *int       `json:"outboxKafkaPartition,omitempty"`
	OutboxKafkaOffset      *int64     `json:"outboxKafkaOffset,omitempty"`
	OutboxPublishedAt      *time.Time `json:"outboxPublishedAt,omitempty"`
	OutboxLastError        *string    `json:"outboxLastError,omitempty"`
	ConsumerName           *string    `json:"consumerName,omitempty"`
	ConsumerStatus         *string    `json:"consumerStatus,omitempty"`
	ConsumerPartitionNo    *int       `json:"consumerPartitionNo,omitempty"`
	ConsumerOffsetNo       *int64     `json:"consumerOffsetNo,omitempty"`
	ConsumerErrorMessage   *string    `json:"consumerErrorMessage,omitempty"`
	ConsumerProcessedAt    *time.Time `json:"consumerProcessedAt,omitempty"`
}

type ManagedWorker struct {
	Name               string          `json:"name"`
	DisplayName        string          `json:"displayName"`
	WorkerType         string          `json:"workerType"`
	DesiredState       string          `json:"desiredState"`
	RuntimeState       string          `json:"runtimeState"`
	OwnerInstanceID    *string         `json:"ownerInstanceId,omitempty"`
	LastHeartbeatAt    *time.Time      `json:"lastHeartbeatAt,omitempty"`
	LastStartedAt      *time.Time      `json:"lastStartedAt,omitempty"`
	LastStoppedAt      *time.Time      `json:"lastStoppedAt,omitempty"`
	LastRunAt          *time.Time      `json:"lastRunAt,omitempty"`
	LastError          *string         `json:"lastError,omitempty"`
	RunOnceRequestedAt *time.Time      `json:"runOnceRequestedAt,omitempty"`
	RunOnceClaimedAt   *time.Time      `json:"runOnceClaimedAt,omitempty"`
	Config             json.RawMessage `json:"config"`
	UpdatedAt          time.Time       `json:"updatedAt"`
}

type WorkerTarget struct {
	TargetKey          string          `json:"targetKey"`
	DisplayName        string          `json:"displayName"`
	Environment        string          `json:"environment"`
	Driver             string          `json:"driver"`
	Enabled            bool            `json:"enabled"`
	MinCapacity        int             `json:"minCapacity"`
	MaxCapacity        int             `json:"maxCapacity"`
	DesiredCapacity    int             `json:"desiredCapacity"`
	ScaleInIdleSeconds int             `json:"scaleInIdleSeconds"`
	Config             json.RawMessage `json:"config"`
	CreatedAt          time.Time       `json:"createdAt"`
	UpdatedAt          time.Time       `json:"updatedAt"`
}

type WorkerPolicy struct {
	ID                     int64           `json:"id"`
	PolicyKey              string          `json:"policyKey"`
	TargetKey              string          `json:"targetKey"`
	Source                 string          `json:"source"`
	WorkerName             string          `json:"workerName"`
	WorkerRole             string          `json:"workerRole"`
	Enabled                bool            `json:"enabled"`
	Mode                   string          `json:"mode"`
	Topic                  *string         `json:"topic,omitempty"`
	ConsumerGroup          *string         `json:"consumerGroup,omitempty"`
	ScaleOutLagThreshold   int64           `json:"scaleOutLagThreshold"`
	ScaleOutQueueThreshold int64           `json:"scaleOutQueueThreshold"`
	BatchSize              int             `json:"batchSize"`
	MaxConcurrency         int             `json:"maxConcurrency"`
	ScaleInIdleSeconds     *int            `json:"scaleInIdleSeconds,omitempty"`
	AllowedStartTime       *string         `json:"allowedStartTime,omitempty"`
	AllowedEndTime         *string         `json:"allowedEndTime,omitempty"`
	CheckIntervalHours     *int            `json:"checkIntervalHours,omitempty"`
	Priority               int             `json:"priority"`
	Config                 json.RawMessage `json:"config"`
	CreatedAt              time.Time       `json:"createdAt"`
	UpdatedAt              time.Time       `json:"updatedAt"`
}

type UpdateWorkerPolicyRequest struct {
	Enabled                bool            `json:"enabled"`
	ScaleOutLagThreshold   int64           `json:"scaleOutLagThreshold"`
	ScaleOutQueueThreshold int64           `json:"scaleOutQueueThreshold"`
	BatchSize              int             `json:"batchSize"`
	MaxConcurrency         int             `json:"maxConcurrency"`
	ScaleInIdleSeconds     *int            `json:"scaleInIdleSeconds,omitempty"`
	AllowedStartTime       *string         `json:"allowedStartTime,omitempty"`
	AllowedEndTime         *string         `json:"allowedEndTime,omitempty"`
	CheckIntervalHours     *int            `json:"checkIntervalHours,omitempty"`
	Priority               int             `json:"priority"`
	Config                 json.RawMessage `json:"config"`
}

type WorkerNodeHeartbeat struct {
	NodeName                string          `json:"nodeName"`
	TargetKey               string          `json:"targetKey"`
	Status                  string          `json:"status"`
	Idle                    bool            `json:"idle"`
	AcceptingNewWork        bool            `json:"acceptingNewWork"`
	RunningWorkers          json.RawMessage `json:"runningWorkers"`
	RunningJobItems         int             `json:"runningJobItems"`
	ProcessingSourceObjects int             `json:"processingSourceObjects"`
	LastHeartbeatAt         time.Time       `json:"lastHeartbeatAt"`
	Metadata                json.RawMessage `json:"metadata"`
	CreatedAt               time.Time       `json:"createdAt"`
	UpdatedAt               time.Time       `json:"updatedAt"`
}

type CollectionPlannerRun struct {
	ID             int64           `json:"id"`
	Source         string          `json:"source"`
	PlannerType    string          `json:"plannerType"`
	Status         string          `json:"status"`
	DetectedCount  int             `json:"detectedCount"`
	PlannedCount   int             `json:"plannedCount"`
	SkippedCount   int             `json:"skippedCount"`
	Reason         *string         `json:"reason,omitempty"`
	PolicySnapshot json.RawMessage `json:"policySnapshot"`
	Details        json.RawMessage `json:"details"`
	PipelineJobID  *int64          `json:"pipelineJobId,omitempty"`
	StartedAt      time.Time       `json:"startedAt"`
	CompletedAt    *time.Time      `json:"completedAt,omitempty"`
	ErrorMessage   *string         `json:"errorMessage,omitempty"`
	CreatedAt      time.Time       `json:"createdAt"`
}

type WorkerScaleCommand struct {
	ID                int64           `json:"id"`
	TargetKey         string          `json:"targetKey"`
	CommandType       string          `json:"commandType"`
	RequestedCapacity int             `json:"requestedCapacity"`
	PreviousCapacity  *int            `json:"previousCapacity,omitempty"`
	Reason            string          `json:"reason"`
	TriggeredBy       string          `json:"triggeredBy"`
	Status            string          `json:"status"`
	PlannerRunID      *int64          `json:"plannerRunId,omitempty"`
	Details           json.RawMessage `json:"details"`
	RequestedAt       time.Time       `json:"requestedAt"`
	StartedAt         *time.Time      `json:"startedAt,omitempty"`
	CompletedAt       *time.Time      `json:"completedAt,omitempty"`
	ErrorMessage      *string         `json:"errorMessage,omitempty"`
}

type WorkerScaleCommandExecution struct {
	Command WorkerScaleCommand `json:"command"`
	Target  WorkerTarget       `json:"target"`
}

type WorkerAgentPolicies struct {
	Target                   WorkerTarget   `json:"target"`
	Policies                 []WorkerPolicy `json:"policies"`
	RefreshIntervalSeconds   int            `json:"refreshIntervalSeconds"`
	HeartbeatIntervalSeconds int            `json:"heartbeatIntervalSeconds"`
}

type KafkaLagSnapshot struct {
	ID            int64           `json:"id"`
	Topic         string          `json:"topic"`
	ConsumerGroup string          `json:"consumerGroup"`
	TotalLag      int64           `json:"totalLag"`
	ThresholdLag  int64           `json:"thresholdLag"`
	Status        string          `json:"status"`
	PartitionLags json.RawMessage `json:"partitionLags"`
	MeasuredAt    time.Time       `json:"measuredAt"`
}

type KafkaLagSummary struct {
	Latest  *KafkaLagSnapshot  `json:"latest,omitempty"`
	History []KafkaLagSnapshot `json:"history"`
}

type Coverage struct {
	ID              int64           `json:"id"`
	DatasetType     string          `json:"datasetType"`
	EntityType      string          `json:"entityType"`
	EntityKey       string          `json:"entityKey"`
	PeriodType      string          `json:"periodType"`
	PeriodStart     time.Time       `json:"periodStart"`
	PeriodEnd       time.Time       `json:"periodEnd"`
	Status          string          `json:"status"`
	ExpectedCount   int             `json:"expectedCount"`
	ActualCount     int             `json:"actualCount"`
	MissingItems    json.RawMessage `json:"missingItems,omitempty"`
	LatestSourceAt  *time.Time      `json:"latestSourceAt,omitempty"`
	LastCheckedAt   time.Time       `json:"lastCheckedAt"`
	ResolutionJobID *int64          `json:"resolutionJobId,omitempty"`
	ReasonCode      *string         `json:"reasonCode,omitempty"`
}

type Gap struct {
	ID              int64           `json:"id"`
	CoverageID      int64           `json:"coverageId"`
	GapKey          string          `json:"gapKey"`
	Status          string          `json:"status"`
	DetectedAt      time.Time       `json:"detectedAt"`
	ResolvedAt      *time.Time      `json:"resolvedAt,omitempty"`
	ResolutionJobID *int64          `json:"resolutionJobId,omitempty"`
	Details         json.RawMessage `json:"details,omitempty"`
}

type Summary struct {
	SchedulesEnabled int64            `json:"schedulesEnabled"`
	JobsByStatus     map[string]int64 `json:"jobsByStatus"`
	OutboxByStatus   map[string]int64 `json:"outboxByStatus"`
	CoverageByStatus map[string]int64 `json:"coverageByStatus"`
	OpenGaps         int64            `json:"openGaps"`
}

type MacroSeries struct {
	ID                 int64   `json:"id"`
	Provider           string  `json:"provider"`
	ExternalSeriesID   string  `json:"externalSeriesId"`
	Name               string  `json:"name"`
	Category           string  `json:"category"`
	Frequency          string  `json:"frequency"`
	Unit               string  `json:"unit"`
	SeasonalAdjustment *string `json:"seasonalAdjustment,omitempty"`
	CountryCode        string  `json:"countryCode"`
}

type BLSInventory struct {
	TotalSeries        int64                  `json:"totalSeries"`
	TotalObservations  int64                  `json:"totalObservations"`
	MinReferencePeriod *string                `json:"minReferencePeriod,omitempty"`
	MaxReferencePeriod *string                `json:"maxReferencePeriod,omitempty"`
	LastCollectedAt    *time.Time             `json:"lastCollectedAt,omitempty"`
	Categories         []BLSInventoryCategory `json:"categories"`
	Series             []BLSInventorySeries   `json:"series"`
}

type BLSInventoryCategory struct {
	Category           string     `json:"category"`
	SeriesCount        int64      `json:"seriesCount"`
	ObservationCount   int64      `json:"observationCount"`
	MinReferencePeriod *string    `json:"minReferencePeriod,omitempty"`
	MaxReferencePeriod *string    `json:"maxReferencePeriod,omitempty"`
	LastCollectedAt    *time.Time `json:"lastCollectedAt,omitempty"`
}

type BLSInventorySeries struct {
	ID                 int64              `json:"id"`
	ExternalSeriesID   string             `json:"externalSeriesId"`
	Name               string             `json:"name"`
	Category           string             `json:"category"`
	Frequency          string             `json:"frequency"`
	Unit               string             `json:"unit"`
	ObservationCount   int64              `json:"observationCount"`
	MinReferencePeriod *string            `json:"minReferencePeriod,omitempty"`
	MaxReferencePeriod *string            `json:"maxReferencePeriod,omitempty"`
	LastCollectedAt    *time.Time         `json:"lastCollectedAt,omitempty"`
	Yearly             []BLSInventoryYear `json:"yearly"`
}

type BLSInventoryYear struct {
	Year               string                          `json:"year"`
	ObservationCount   int64                           `json:"observationCount"`
	ExpectedCount      int64                           `json:"expectedCount"`
	UnavailableCount   int64                           `json:"unavailableCount"`
	Periods            []string                        `json:"periods"`
	UnavailablePeriods []BLSInventoryUnavailablePeriod `json:"unavailablePeriods"`
}

type BLSInventoryUnavailablePeriod struct {
	ReferencePeriod string  `json:"referencePeriod"`
	Status          string  `json:"status"`
	ReasonCode      *string `json:"reasonCode,omitempty"`
	ReasonMessage   *string `json:"reasonMessage,omitempty"`
	RawValue        *string `json:"rawValue,omitempty"`
}

type AssetCompanyLinkSummary struct {
	TotalAssets      int64            `json:"totalAssets"`
	EquityAssets     int64            `json:"equityAssets"`
	LinkedAssets     int64            `json:"linkedAssets"`
	UnlinkedEquities int64            `json:"unlinkedEquities"`
	MatchesByStatus  map[string]int64 `json:"matchesByStatus"`
}

type PriceQualitySummary struct {
	TotalBars        int64            `json:"totalBars"`
	ByQualityStatus  map[string]int64 `json:"byQualityStatus"`
	ByDisplayPolicy  map[string]int64 `json:"byDisplayPolicy"`
	OpenIssues       int64            `json:"openIssues"`
	IssuesBySeverity map[string]int64 `json:"issuesBySeverity"`
	IssuesByCode     map[string]int64 `json:"issuesByCode"`
	ChartUsableBars  int64            `json:"chartUsableBars"`
	CandleUsableBars int64            `json:"candleUsableBars"`
	HiddenBars       int64            `json:"hiddenBars"`
}

type PriceBarInventory struct {
	TotalBars       int64                    `json:"totalBars"`
	AssetsWithBars  int64                    `json:"assetsWithBars"`
	MinBarTime      *time.Time               `json:"minBarTime,omitempty"`
	MaxBarTime      *time.Time               `json:"maxBarTime,omitempty"`
	LastCollectedAt *time.Time               `json:"lastCollectedAt,omitempty"`
	Rows            []PriceBarInventoryAsset `json:"rows"`
}

type PriceBarInventoryAsset struct {
	AssetID         int64      `json:"assetId"`
	Ticker          string     `json:"ticker"`
	Name            string     `json:"name"`
	AssetType       string     `json:"assetType"`
	Exchange        string     `json:"exchange"`
	Source          string     `json:"source"`
	Interval        string     `json:"interval"`
	BarCount        int64      `json:"barCount"`
	MinBarTime      *time.Time `json:"minBarTime,omitempty"`
	MaxBarTime      *time.Time `json:"maxBarTime,omitempty"`
	LastCollectedAt *time.Time `json:"lastCollectedAt,omitempty"`
}

type AssetCompanyMatch struct {
	ID                   int64           `json:"id"`
	AssetID              int64           `json:"assetId"`
	AssetTicker          string          `json:"assetTicker"`
	AssetName            string          `json:"assetName"`
	AssetType            string          `json:"assetType"`
	Exchange             string          `json:"exchange"`
	CompanyID            int64           `json:"companyId"`
	CIK                  string          `json:"cik"`
	CompanyName          string          `json:"companyName"`
	MatchMethod          string          `json:"matchMethod"`
	MatchConfidence      float64         `json:"matchConfidence"`
	Status               string          `json:"status"`
	SECSourceObjectID    *int64          `json:"secSourceObjectId,omitempty"`
	MarketSourceObjectID *int64          `json:"marketSourceObjectId,omitempty"`
	Evidence             json.RawMessage `json:"evidence"`
	MatchedAt            time.Time       `json:"matchedAt"`
	ReviewedAt           *time.Time      `json:"reviewedAt,omitempty"`
	ReviewedBy           *string         `json:"reviewedBy,omitempty"`
	CreatedAt            time.Time       `json:"createdAt"`
	UpdatedAt            time.Time       `json:"updatedAt"`
}

type UnmatchedAsset struct {
	ID                   int64  `json:"id"`
	Ticker               string `json:"ticker"`
	Name                 string `json:"name"`
	AssetType            string `json:"assetType"`
	Exchange             string `json:"exchange"`
	Provider             string `json:"provider,omitempty"`
	ProviderExchangeCode string `json:"providerExchangeCode,omitempty"`
	ProviderSymbol       string `json:"providerSymbol,omitempty"`
}

type AssetOption struct {
	ID                   int64  `json:"id"`
	Ticker               string `json:"ticker"`
	Name                 string `json:"name"`
	AssetType            string `json:"assetType"`
	Exchange             string `json:"exchange"`
	Currency             string `json:"currency"`
	Provider             string `json:"provider,omitempty"`
	ProviderExchangeCode string `json:"providerExchangeCode,omitempty"`
	ProviderSymbol       string `json:"providerSymbol,omitempty"`
}

type PriceQualityIssue struct {
	ID            int64      `json:"id"`
	PriceBarID    int64      `json:"priceBarId"`
	AssetID       int64      `json:"assetId"`
	Ticker        string     `json:"ticker"`
	AssetName     string     `json:"assetName"`
	Exchange      string     `json:"exchange"`
	Source        string     `json:"source"`
	Interval      string     `json:"interval"`
	BarTime       time.Time  `json:"barTime"`
	QualityStatus string     `json:"qualityStatus"`
	DisplayPolicy string     `json:"displayPolicy"`
	IssueCode     string     `json:"issueCode"`
	FieldName     string     `json:"fieldName"`
	Severity      string     `json:"severity"`
	IssueStatus   string     `json:"issueStatus"`
	ObservedValue *string    `json:"observedValue,omitempty"`
	DetectedAt    time.Time  `json:"detectedAt"`
	LastSeenAt    time.Time  `json:"lastSeenAt"`
	ResolvedAt    *time.Time `json:"resolvedAt,omitempty"`
}

type CreateScheduleRequest struct {
	JobType        string          `json:"jobType"`
	CronExpression string          `json:"cronExpression"`
	Timezone       string          `json:"timezone"`
	Enabled        bool            `json:"enabled"`
	Parameters     json.RawMessage `json:"parameters"`
}

type UpdateScheduleRequest struct {
	JobType        string          `json:"jobType"`
	CronExpression string          `json:"cronExpression"`
	Timezone       string          `json:"timezone"`
	Parameters     json.RawMessage `json:"parameters"`
}

type CreateJobRequest struct {
	JobType        string          `json:"jobType"`
	Source         string          `json:"source"`
	Priority       int             `json:"priority"`
	ScheduledAt    *time.Time      `json:"scheduledAt,omitempty"`
	RequestedBy    string          `json:"requestedBy"`
	IdempotencyKey string          `json:"idempotencyKey"`
	Parameters     json.RawMessage `json:"parameters"`
	ItemKeys       []string        `json:"itemKeys"`
}

type WorkerPolicyFilter struct {
	TargetKey string
	Source    string
	Limit     int
	Offset    int
}

type WorkerOpsFilter struct {
	TargetKey string
	Source    string
	Status    string
	Limit     int
	Offset    int
}

type UpdateWorkerTargetRequest struct {
	DisplayName        string          `json:"displayName"`
	Environment        string          `json:"environment"`
	Driver             string          `json:"driver"`
	Enabled            bool            `json:"enabled"`
	MinCapacity        int             `json:"minCapacity"`
	MaxCapacity        int             `json:"maxCapacity"`
	DesiredCapacity    int             `json:"desiredCapacity"`
	ScaleInIdleSeconds int             `json:"scaleInIdleSeconds"`
	Config             json.RawMessage `json:"config"`
}

type AdminLoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type AdminAuthResponse struct {
	Authenticated bool       `json:"authenticated"`
	Actor         string     `json:"actor"`
	ExpiresAt     *time.Time `json:"expiresAt,omitempty"`
}

type AdminSession struct {
	ID               int64      `json:"id"`
	SessionTokenHash string     `json:"-"`
	Actor            string     `json:"actor"`
	IPAddress        *string    `json:"ipAddress,omitempty"`
	UserAgent        *string    `json:"userAgent,omitempty"`
	CreatedAt        time.Time  `json:"createdAt"`
	LastSeenAt       time.Time  `json:"lastSeenAt"`
	ExpiresAt        time.Time  `json:"expiresAt"`
	RevokedAt        *time.Time `json:"revokedAt,omitempty"`
}

type CreateAdminSessionRequest struct {
	SessionTokenHash string
	Actor            string
	IPAddress        *string
	UserAgent        *string
	ExpiresAt        time.Time
}

type WorkerHeartbeatRequest struct {
	NodeName                string          `json:"nodeName"`
	TargetKey               string          `json:"targetKey"`
	Status                  string          `json:"status"`
	Idle                    bool            `json:"idle"`
	AcceptingNewWork        bool            `json:"acceptingNewWork"`
	RunningWorkers          json.RawMessage `json:"runningWorkers"`
	RunningJobItems         int             `json:"runningJobItems"`
	ProcessingSourceObjects int             `json:"processingSourceObjects"`
	Metadata                json.RawMessage `json:"metadata"`
}

type CreateWorkerScaleCommandRequest struct {
	RequestedCapacity int             `json:"requestedCapacity"`
	Reason            string          `json:"reason"`
	TriggeredBy       string          `json:"triggeredBy"`
	Details           json.RawMessage `json:"details"`
}

type RunCollectionPlannerRequest struct {
	Source      string          `json:"source"`
	PlannerType string          `json:"plannerType"`
	Details     json.RawMessage `json:"details"`
}

type BackfillGapRequest struct {
	JobType     string          `json:"jobType"`
	Source      string          `json:"source"`
	Priority    int             `json:"priority"`
	RequestedBy string          `json:"requestedBy"`
	Parameters  json.RawMessage `json:"parameters"`
	ItemKeys    []string        `json:"itemKeys"`
}

type ReviewAssetCompanyMatchRequest struct {
	ReviewedBy string `json:"reviewedBy"`
}

type ListFilter struct {
	Status string
	Limit  int
	Offset int
}

type AssetLinkFilter struct {
	Status    string
	AssetType string
	Exchange  string
	Query     string
	Limit     int
	Offset    int
}

type PriceQualityIssueFilter struct {
	Status   string
	Severity string
	Query    string
	Limit    int
	Offset   int
}

type ScheduleParameters struct {
	Source   string   `json:"source"`
	ItemKeys []string `json:"itemKeys"`
}
