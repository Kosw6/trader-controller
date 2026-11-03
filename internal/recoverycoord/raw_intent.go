package recoverycoord

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
)

type RawSource struct {
	ID        int64  `json:"id"`
	Hash      string `json:"hash"`
	DedupeKey string `json:"dedupeKey"`
	Size      int64  `json:"size"`
	DataType  string `json:"dataType"`
}

// DispatchRaw never fetches a provider. The intent, receipt and outbox transition
// are fenced in one transaction. A lost response is reconciled by ReadReceipt.
func (c *Coordinator) DispatchRaw(ctx context.Context, l Lease) error {
	w, err := c.Read(ctx, l.WorkKey)
	if err != nil {
		return err
	}
	rawTopic := c.rawTopic
	if w.Identity.Provider == "NEWS" {
		rawTopic = strings.Replace(c.rawTopic, "coordinated.ready", "coordinated.news.ready", 1)
	}
	var rawHash string
	err = c.pool.QueryRow(ctx, `SELECT r.content_hash FROM pipeline_recovery_raw_intent i
 JOIN pipeline_recovery_receipt r ON (r.work_key,r.receipt_id)=(i.work_key,i.receipt_id)
 WHERE i.work_key=$1 AND r.from_phase='FETCH_RAW' AND r.to_phase='RAW_READY'`, l.WorkKey).Scan(&rawHash)
	if err != nil {
		return err
	}
	return c.Apply(ctx, l, Receipt{ID: "outbox-" + rawHash, From: "RAW_READY", To: "ETL_READY", Hash: rawHash}, func(ctx context.Context, tx SQL) error {
		var item, job int64
		var sourcesJSON, contractJSON []byte
		var dispatched *int64
		err := tx.QueryRow(ctx, `SELECT item_id,job_id,sources,contract,outbox_id
   FROM pipeline_recovery_raw_intent WHERE work_key=$1 FOR UPDATE`, l.WorkKey).Scan(&item, &job, &sourcesJSON, &contractJSON, &dispatched)
		if err != nil {
			return err
		}
		if dispatched != nil {
			return ErrReview
		}
		var jobStatus, itemStatus string
		// Same job -> item lock order as the raw writer.
		if err = tx.QueryRow(ctx, `SELECT status FROM pipeline_job WHERE id=$1 FOR UPDATE`, job).Scan(&jobStatus); err != nil {
			return err
		}
		if err = tx.QueryRow(ctx, `SELECT status FROM pipeline_job_item WHERE id=$1 AND job_id=$2 FOR UPDATE`, item, job).Scan(&itemStatus); err != nil {
			return err
		}
		if (jobStatus != "RUNNING" && jobStatus != "SUCCESS" && jobStatus != "PARTIAL_SUCCESS") || itemStatus != "COLLECTED" {
			return ErrReview
		}
		var sources []RawSource
		if json.Unmarshal(sourcesJSON, &sources) != nil || len(sources) == 0 {
			return ErrConflict
		}
		seen := map[int64]bool{}
		for _, source := range sources {
			if source.ID <= 0 || seen[source.ID] || !digest.MatchString(source.Hash) || source.Size < 0 {
				return ErrConflict
			}
			seen[source.ID] = true
			var provider, hashValue, dedupe, dataType string
			var size int64
			if err = tx.QueryRow(ctx, `SELECT provider,content_hash,object_size,dedupe_key,data_type FROM source_object WHERE id=$1 FOR SHARE`, source.ID).Scan(&provider, &hashValue, &size, &dedupe, &dataType); err != nil {
				return err
			}
			if provider != w.Identity.Provider || hashValue != source.Hash || size != source.Size || dedupe != source.DedupeKey || dataType != source.DataType {
				return ErrConflict
			}
		}
		key := "COORDINATED_RAW_READY:" + l.WorkKey
		payload, err := json.Marshal(map[string]any{"eventVersion": 2, "eventType": "COORDINATED_RAW_READY", "workKey": l.WorkKey, "rawHash": rawHash, "sources": sources, "contract": json.RawMessage(contractJSON), "itemId": item, "jobId": job})
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO pipeline_outbox(source_object_id,job_item_id,event_type,topic,message_key,idempotency_key,payload,status,attempt_count,next_attempt_at)
   VALUES($1,$2,'COORDINATED_RAW_READY',$3,$4,$5,$6,'PENDING',0,clock_timestamp()) ON CONFLICT(idempotency_key) DO NOTHING`, sources[0].ID, item, rawTopic, l.WorkKey, key, payload)
		if err != nil {
			return err
		}
		var outboxID int64
		var actualJSON []byte
		var topic, eventType, messageKey string
		if err = tx.QueryRow(ctx, `SELECT id,payload,topic,event_type,message_key FROM pipeline_outbox WHERE idempotency_key=$1 FOR SHARE`, key).Scan(&outboxID, &actualJSON, &topic, &eventType, &messageKey); err != nil {
			return err
		}
		var expected, actual any
		if json.Unmarshal(payload, &expected) != nil || json.Unmarshal(actualJSON, &actual) != nil || !reflect.DeepEqual(expected, actual) || topic != rawTopic || eventType != "COORDINATED_RAW_READY" || messageKey != l.WorkKey {
			return ErrConflict
		}
		_, err = tx.Exec(ctx, `UPDATE pipeline_recovery_raw_intent SET outbox_id=$2,dispatched_at=clock_timestamp() WHERE work_key=$1`, l.WorkKey, outboxID)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE pipeline_job_item SET checkpoint=jsonb_set(checkpoint,'{rawReadyNotificationPending}','false'::jsonb) WHERE id=$1`, item)
		return err
	})
}
