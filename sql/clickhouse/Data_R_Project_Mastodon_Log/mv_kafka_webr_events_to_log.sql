DROP TABLE IF EXISTS Data_R_Project_Mastodon_Log.mv_kafka_webr_events_to_log ON CLUSTER statground_cluster;

CREATE MATERIALIZED VIEW Data_R_Project_Mastodon_Log.mv_kafka_webr_events_to_log
ON CLUSTER statground_cluster
TO Data_R_Project_Mastodon_Log.log
AS
SELECT
    ifNull(toUUIDOrNull(JSONExtractString(k.payload, 'uuid')), toUUID(k.event_uuid)) AS uuid,
    parseDateTime64BestEffortOrNull(JSONExtractString(k.payload, 'created_at'), 3, 'Asia/Seoul') AS created_at,
    CAST(nullIf(nullIf(JSONExtractRaw(k.payload, 'created_log'), ''), 'null'), 'Nullable(JSON)') AS created_log,
    ifNull(nullIf(JSONExtractString(k.payload, 'language_code'), ''), 'en') AS language_code
FROM Data_R_Project_Mastodon_Log.kafka_webr_events_queue AS k
WHERE length(_error) = 0
  AND k.event_type = 'webr.mastodon.log.v1';

ALTER TABLE Data_R_Project_Mastodon_Log.mv_kafka_webr_events_to_log
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Kafka webr.mastodon.log.v1 events to Data_R_Project_Mastodon_Log.log';
