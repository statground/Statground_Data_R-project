DROP TABLE IF EXISTS Data_R_Blogger_Log.mv_kafka_webr_events_to_raw ON CLUSTER statground_cluster;

CREATE MATERIALIZED VIEW Data_R_Blogger_Log.mv_kafka_webr_events_to_raw
ON CLUSTER statground_cluster
TO Data_R_Blogger_Raw.raw
AS
SELECT
    ifNull(toUUIDOrNull(JSONExtractString(k.payload, 'uuid')), toUUID(k.event_uuid)) AS uuid,
    parseDateTime64BestEffortOrNull(JSONExtractString(k.payload, 'created_at'), 3, 'Asia/Seoul') AS created_at,
    CAST(nullIf(nullIf(JSONExtractRaw(k.payload, 'created_log'), ''), 'null'), 'Nullable(JSON)') AS created_log,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(k.payload, 'updated_at'), ''), 3, 'Asia/Seoul') AS updated_at,
    CAST(nullIf(nullIf(JSONExtractRaw(k.payload, 'updated_log'), ''), 'null'), 'Nullable(JSON)') AS updated_log,
    toUInt8OrNull(nullIf(nullIf(JSONExtractRaw(k.payload, 'active'), ''), 'null')) AS active,
    nullIf(JSONExtractString(k.payload, 'github_path'), '') AS github_path,
    nullIf(JSONExtractString(k.payload, 'title'), '') AS title,
    nullIf(JSONExtractString(k.payload, 'content'), '') AS content,
    nullIf(JSONExtractString(k.payload, 'url'), '') AS url,
    ifNull(nullIf(JSONExtractString(k.payload, 'url_hash'), ''), k.event_uuid) AS url_hash,
    ifNull(nullIf(JSONExtractString(k.payload, 'language_code'), ''), 'en') AS language_code
FROM Data_R_Blogger_Log.kafka_webr_events_queue AS k
WHERE k.event_type = 'webr.rblogger.raw.v1';

ALTER TABLE Data_R_Blogger_Log.mv_kafka_webr_events_to_raw
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Kafka webr.rblogger.raw.v1 events to Data_R_Blogger_Raw.raw';
