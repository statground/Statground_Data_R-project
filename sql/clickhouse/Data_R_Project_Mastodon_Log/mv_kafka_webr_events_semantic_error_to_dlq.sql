DROP TABLE IF EXISTS Data_R_Project_Mastodon_Log.mv_kafka_webr_events_semantic_error_to_dlq ON CLUSTER statground_cluster;

CREATE MATERIALIZED VIEW Data_R_Project_Mastodon_Log.mv_kafka_webr_events_semantic_error_to_dlq
ON CLUSTER statground_cluster
TO Data_R_Project_Mastodon_Log.kafka_ingest_error
AS
SELECT
    toString(k.event_uuid) AS event_uuid_raw,
    _topic AS kafka_topic,
    toUInt32(_partition) AS kafka_partition,
    toUInt64(_offset) AS kafka_offset,
    k.event_type AS event_type,
    k.payload AS raw_message,
    concat('Mastodon semantic validation failed for event_type=', k.event_type) AS error_message,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM Data_R_Project_Mastodon_Log.kafka_webr_events_queue AS k
WHERE length(_error) = 0
  AND startsWith(k.event_type, 'webr.mastodon.')
  AND (
      k.event_type NOT IN ('webr.mastodon.raw.v1', 'webr.mastodon.log.v1', 'webr.mastodon.board.v1')
      OR toUUIDOrNull(toString(k.event_uuid)) IS NULL
      OR (
          k.event_type = 'webr.mastodon.raw.v1'
          AND (
              toUUIDOrNull(JSONExtractString(k.payload, 'uuid')) IS NULL
              OR JSONExtractString(k.payload, 'instance_host') = ''
              OR JSONExtractString(k.payload, 'account_acct') = ''
              OR JSONExtractString(k.payload, 'account_id') = ''
              OR JSONExtractString(k.payload, 'status_id') = ''
          )
      )
      OR (
          k.event_type IN ('webr.mastodon.log.v1', 'webr.mastodon.board.v1')
          AND toUUIDOrNull(JSONExtractString(k.payload, 'uuid')) IS NULL
      )
  );

ALTER TABLE Data_R_Project_Mastodon_Log.mv_kafka_webr_events_semantic_error_to_dlq
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view capturing semantic validation failures from Mastodon Kafka events into Data_R_Project_Mastodon_Log.kafka_ingest_error';
