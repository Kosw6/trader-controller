UPDATE source_object
SET processing_attempt_count = 1
WHERE processing_attempt_count = 0
  AND processing_status IN ('PROCESSING', 'PROCESSED', 'FAILED');
