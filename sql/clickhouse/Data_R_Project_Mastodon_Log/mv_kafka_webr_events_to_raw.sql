DROP TABLE IF EXISTS Data_R_Project_Mastodon_Log.mv_kafka_webr_events_to_raw ON CLUSTER statground_cluster;

CREATE MATERIALIZED VIEW Data_R_Project_Mastodon_Log.mv_kafka_webr_events_to_raw
ON CLUSTER statground_cluster
TO Data_R_Project_Mastodon_Raw.raw
AS
SELECT
    assumeNotNull(toUUIDOrNull(JSONExtractString(k.payload, 'uuid'))) AS uuid,
    assumeNotNull(toUUIDOrNull(toString(k.event_uuid))) AS event_uuid,
    k.source AS source,
    k.host AS host,
    toUUIDOrNull(nullIf(k.uuid_user, '')) AS uuid_user,
    toIPv6OrDefault(k.ip) AS ip,
    k.url AS url,
    k.event_type AS event_type,

    JSONExtractString(k.payload, 'instance_host') AS instance_host,
    JSONExtractString(k.payload, 'account_acct') AS account_acct,
    JSONExtractString(k.payload, 'account_id') AS account_id,
    JSONExtractString(k.payload, 'status_id') AS status_id,
    JSONExtractString(k.payload, 'status_uri') AS status_uri,
    JSONExtractString(k.payload, 'status_url') AS status_url,
    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(k.payload, 'status_created_at'), 3, 'Asia/Seoul'),
        parseDateTime64BestEffortOrNull(JSONExtractString(k.payload, 'fetched_at'), 3, 'Asia/Seoul'),
        parseDateTime64BestEffortOrNull(k.created_at, 3, 'Asia/Seoul'),
        now64(3, 'Asia/Seoul')
    ) AS status_created_at,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(k.payload, 'status_edited_at'), ''), 3, 'Asia/Seoul') AS status_edited_at,
    ifNull(nullIf(JSONExtractString(k.payload, 'visibility'), ''), 'unknown') AS visibility,
    ifNull(JSONExtractString(k.payload, 'language'), '') AS language,
    ifNull(nullIf(JSONExtractString(k.payload, 'language_code'), ''), 'en') AS language_code,
    JSONExtract(k.payload, 'sensitive', 'UInt8') AS sensitive,
    ifNull(JSONExtractString(k.payload, 'spoiler_text'), '') AS spoiler_text,
    ifNull(JSONExtractString(k.payload, 'content_html'), '') AS content_html,
    ifNull(JSONExtractString(k.payload, 'content_text'), '') AS content_text,
    nullIf(JSONExtractString(k.payload, 'in_reply_to_id'), '') AS in_reply_to_id,
    nullIf(JSONExtractString(k.payload, 'in_reply_to_account_id'), '') AS in_reply_to_account_id,
    JSONExtract(k.payload, 'is_reblog', 'UInt8') AS is_reblog,
    nullIf(JSONExtractString(k.payload, 'reblog_status_id'), '') AS reblog_status_id,
    JSONExtract(k.payload, 'replies_count', 'UInt32') AS replies_count,
    JSONExtract(k.payload, 'reblogs_count', 'UInt32') AS reblogs_count,
    JSONExtract(k.payload, 'favourites_count', 'UInt32') AS favourites_count,
    JSONExtract(k.payload, 'active', 'UInt8') AS active,

    if(JSONExtractRaw(k.payload, 'tags') = '', '[]', JSONExtractRaw(k.payload, 'tags')) AS tags_json,
    if(JSONExtractRaw(k.payload, 'mentions') = '', '[]', JSONExtractRaw(k.payload, 'mentions')) AS mentions_json,
    if(JSONExtractRaw(k.payload, 'emojis') = '', '[]', JSONExtractRaw(k.payload, 'emojis')) AS emojis_json,
    if(JSONExtractRaw(k.payload, 'media_attachments') = '', '[]', JSONExtractRaw(k.payload, 'media_attachments')) AS media_attachments_json,
    if(JSONExtractRaw(k.payload, 'card') = '', '{}', JSONExtractRaw(k.payload, 'card')) AS card_json,
    if(JSONExtractRaw(k.payload, 'poll') = '', '{}', JSONExtractRaw(k.payload, 'poll')) AS poll_json,
    if(JSONExtractRaw(k.payload, 'raw_status_json') = '', '{}', JSONExtractRaw(k.payload, 'raw_status_json')) AS raw_status_json,
    JSONExtract(k.payload, 'payload_hash', 'UInt64') AS payload_hash,
    JSONExtract(k.payload, 'image_count', 'UInt8') AS image_count,
    JSONExtract(k.payload, 'image_base64_count', 'UInt8') AS image_base64_count,
    JSONExtract(k.payload, 'has_image_base64', 'UInt8') AS has_image_base64,

    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(k.payload, 'fetched_at'), 3, 'Asia/Seoul'),
        parseDateTime64BestEffortOrNull(k.created_at, 3, 'Asia/Seoul'),
        now64(3, 'Asia/Seoul')
    ) AS fetched_at,
    coalesce(parseDateTime64BestEffortOrNull(k.created_at, 3, 'Asia/Seoul'), now64(3, 'Asia/Seoul')) AS created_at,
    now64(3, 'Asia/Seoul') AS ingested_at,
    _topic AS kafka_topic,
    toUInt32(_partition) AS kafka_partition,
    toUInt64(_offset) AS kafka_offset
FROM Data_R_Project_Mastodon_Log.kafka_webr_events_queue AS k
WHERE length(_error) = 0
  AND k.event_type = 'webr.mastodon.raw.v1'
  AND toUUIDOrNull(toString(k.event_uuid)) IS NOT NULL
  AND toUUIDOrNull(JSONExtractString(k.payload, 'uuid')) IS NOT NULL
  AND JSONExtractString(k.payload, 'instance_host') != ''
  AND JSONExtractString(k.payload, 'account_acct') != ''
  AND JSONExtractString(k.payload, 'account_id') != ''
  AND JSONExtractString(k.payload, 'status_id') != '';

ALTER TABLE Data_R_Project_Mastodon_Log.mv_kafka_webr_events_to_raw
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Kafka webr.mastodon.raw.v1 events to Data_R_Project_Mastodon_Raw.raw; raw table is first persistence layer';
