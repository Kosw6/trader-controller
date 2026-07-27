package postgres

import (
	"context"
	"strings"

	"github.com/kosw006/trader-controller/internal/domain"
)

// ListSourceObjects는 최근 raw 객체의 저장, Kafka 발행, 소비, lineage 상태를 함께 조회한다.
// record_lineage는 ETL 완료 뒤에 생기므로 LEFT JOIN으로 조회해 수집만 끝난 raw도 보이게 둔다.
func (s *Store) ListSourceObjects(ctx context.Context, filter domain.ListFilter) ([]domain.SourceObjectStatus, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT
            so.id,
            so.provider,
            so.data_type,
            so.external_id,
            so.pipeline_job_item_id,
            i.job_id,
            i.item_key,
            j.job_type,
            so.storage_backend,
            so.storage_key,
            so.processing_status,
            CASE
                WHEN so.processing_status = 'FAILED'
                    OR latest_outbox.status = 'DEAD'
                    OR latest_event.status = 'FAILED'
                    THEN 'FAILED'
                WHEN so.processing_status = 'PROCESSED' AND COUNT(rl.id) > 0
                    THEN 'PROCESSED'
                WHEN so.processing_status = 'PROCESSING'
                    THEN 'PROCESSING'
                WHEN so.processing_status = 'COLLECTED' AND latest_outbox.id IS NULL
                    THEN 'COLLECTED'
                WHEN latest_outbox.status IN ('PENDING', 'PUBLISHING', 'RETRY_WAIT')
                    THEN 'RAW_EVENT_PENDING'
                WHEN latest_outbox.status = 'PUBLISHED' AND latest_event.status IS NULL
                    THEN 'CONSUMER_PENDING'
                WHEN latest_event.status = 'SUCCESS' AND COUNT(rl.id) = 0
                    THEN 'LINEAGE_PENDING'
                ELSE COALESCE(so.processing_status, 'PENDING')
            END AS operational_status,
            so.processing_started_at,
            so.processing_finished_at,
            so.processing_error_message,
            so.processing_attempt_count,
            COUNT(rl.id)::BIGINT AS lineage_count,
            COALESCE(string_agg(DISTINCT rl.target_table, ',' ORDER BY rl.target_table), '') AS target_tables,
            so.collected_at,
            MAX(rl.processed_at) AS last_processed_at,
            latest_outbox.id AS outbox_id,
            latest_outbox.status AS outbox_status,
            latest_outbox.topic AS outbox_topic,
            latest_outbox.kafka_partition AS outbox_kafka_partition,
            latest_outbox.kafka_offset AS outbox_kafka_offset,
            latest_outbox.published_at AS outbox_published_at,
            latest_outbox.last_error AS outbox_last_error,
            latest_event.consumer_name,
            latest_event.status AS consumer_status,
            latest_event.partition_no AS consumer_partition_no,
            latest_event.offset_no AS consumer_offset_no,
            latest_event.error_message AS consumer_error_message,
            latest_event.processed_at AS consumer_processed_at
        FROM source_object so
        LEFT JOIN pipeline_job_item i ON i.id = so.pipeline_job_item_id
        LEFT JOIN pipeline_job j ON j.id = i.job_id
        LEFT JOIN record_lineage rl ON rl.source_object_id = so.id
        LEFT JOIN LATERAL (
            SELECT
                po.id,
                po.status,
                po.topic,
                po.kafka_partition,
                po.kafka_offset,
                po.published_at,
                po.last_error,
                po.idempotency_key
            FROM pipeline_outbox po
            WHERE po.source_object_id = so.id
              AND po.event_type = 'RAW_OBJECT_READY'
            ORDER BY po.id DESC
            LIMIT 1
        ) latest_outbox ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                pe.consumer_name,
                pe.status,
                pe.partition_no,
                pe.offset_no,
                pe.error_message,
                pe.processed_at
            FROM processed_event pe
            WHERE pe.idempotency_key = latest_outbox.idempotency_key
            ORDER BY pe.processed_at DESC
            LIMIT 1
        ) latest_event ON TRUE
        WHERE ($1 = '' OR so.processing_status = $1)
        GROUP BY
            so.id,
            so.provider,
            so.data_type,
            so.external_id,
            so.pipeline_job_item_id,
            i.job_id,
            i.item_key,
            j.job_type,
            so.storage_backend,
            so.storage_key,
            so.processing_status,
            so.processing_started_at,
            so.processing_finished_at,
            so.processing_error_message,
            so.processing_attempt_count,
            so.collected_at,
            latest_outbox.id,
            latest_outbox.status,
            latest_outbox.topic,
            latest_outbox.kafka_partition,
            latest_outbox.kafka_offset,
            latest_outbox.published_at,
            latest_outbox.last_error,
            latest_event.consumer_name,
            latest_event.status,
            latest_event.partition_no,
            latest_event.offset_no,
            latest_event.error_message,
            latest_event.processed_at
        ORDER BY so.id DESC
        LIMIT $2 OFFSET $3`,
		filter.Status,
		filter.Limit,
		filter.Offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]domain.SourceObjectStatus, 0)
	for rows.Next() {
		var value domain.SourceObjectStatus
		var targetTables string
		if err := rows.Scan(
			&value.ID,
			&value.Provider,
			&value.DataType,
			&value.ExternalID,
			&value.PipelineJobItemID,
			&value.JobID,
			&value.ItemKey,
			&value.JobType,
			&value.StorageBackend,
			&value.StorageKey,
			&value.ProcessingStatus,
			&value.OperationalStatus,
			&value.ProcessingStartedAt,
			&value.ProcessingFinishedAt,
			&value.ProcessingErrorMessage,
			&value.ProcessingAttemptCount,
			&value.LineageCount,
			&targetTables,
			&value.CollectedAt,
			&value.LastProcessedAt,
			&value.OutboxID,
			&value.OutboxStatus,
			&value.OutboxTopic,
			&value.OutboxKafkaPartition,
			&value.OutboxKafkaOffset,
			&value.OutboxPublishedAt,
			&value.OutboxLastError,
			&value.ConsumerName,
			&value.ConsumerStatus,
			&value.ConsumerPartitionNo,
			&value.ConsumerOffsetNo,
			&value.ConsumerErrorMessage,
			&value.ConsumerProcessedAt,
		); err != nil {
			return nil, err
		}
		if targetTables != "" {
			value.TargetTables = strings.Split(targetTables, ",")
		} else {
			value.TargetTables = []string{}
		}
		result = append(result, value)
	}
	return result, rows.Err()
}
