DROP TABLE IF EXISTS Data_R_Project_Mastodon_Log.mv_kafka_webr_events_parse_error_to_dlq ON CLUSTER statground_cluster;

CREATE MATERIALIZED VIEW Data_R_Project_Mastodon_Log.mv_kafka_webr_events_parse_error_to_dlq
ON CLUSTER statground_cluster
TO Data_R_Project_Mastodon_Log.kafka_ingest_error
AS
SELECT
    toString(event_uuid) AS event_uuid_raw,
    _topic AS kafka_topic,
    toUInt32(_partition) AS kafka_partition,
    toUInt64(_offset) AS kafka_offset,
    event_type AS event_type,
    _raw_message AS raw_message,
    _error AS error_message,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM Data_R_Project_Mastodon_Log.kafka_webr_events_queue
WHERE length(_error) > 0;

ALTER TABLE Data_R_Project_Mastodon_Log.mv_kafka_webr_events_parse_error_to_dlq
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view capturing Kafka JSON parser errors from webr.events into Data_R_Project_Mastodon_Log.kafka_ingest_error';
