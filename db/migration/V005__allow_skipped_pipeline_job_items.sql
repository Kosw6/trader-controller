-- Allow workers to mark intentionally ignored items as SKIPPED.
-- SKIPPED means the item was inspected and deliberately not processed, so it is
-- counted as a non-failure terminal state in job aggregation.

ALTER TABLE pipeline_job_item
    DROP CONSTRAINT ck_pipeline_job_item_status;

ALTER TABLE pipeline_job_item
    ADD CONSTRAINT ck_pipeline_job_item_status
        CHECK (status IN (
            'SCHEDULED', 'QUEUED', 'RUNNING', 'RETRY_WAIT',
            'COLLECTED', 'SUCCESS', 'SKIPPED', 'FAILED', 'CANCELLED'
        ));
