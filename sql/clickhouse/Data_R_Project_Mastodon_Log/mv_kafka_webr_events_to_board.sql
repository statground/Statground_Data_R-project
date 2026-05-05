DROP TABLE IF EXISTS Data_R_Project_Mastodon_Log.mv_kafka_webr_events_to_board ON CLUSTER statground_cluster;

CREATE MATERIALIZED VIEW Data_R_Project_Mastodon_Log.mv_kafka_webr_events_to_board
ON CLUSTER statground_cluster
TO Data_R_Project_Mastodon_Service.board
AS
SELECT
    ifNull(toUUIDOrNull(JSONExtractString(k.payload, 'uuid')), toUUID(k.event_uuid)) AS uuid,
    nullIf(JSONExtractString(k.payload, 'title'), '') AS title,
    nullIf(
        if(
            empty(trimBoth(JSONExtractString(k.payload, 'content'))),
            JSONExtractString(k.payload, 'title'),
            JSONExtractString(k.payload, 'content')
        ),
        ''
    ) AS content,
    toUInt8OrNull(nullIf(nullIf(JSONExtractRaw(k.payload, 'active'), ''), 'null')) AS active,
    parseDateTime64BestEffortOrNull(JSONExtractString(k.payload, 'created_at'), 3, 'Asia/Seoul') AS created_at,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(k.payload, 'updated_at'), ''), 3, 'Asia/Seoul') AS updated_at,
    CAST(nullIf(nullIf(JSONExtractRaw(k.payload, 'created_log'), ''), 'null'), 'Nullable(JSON)') AS created_log,
    CAST(nullIf(nullIf(JSONExtractRaw(k.payload, 'updated_log'), ''), 'null'), 'Nullable(JSON)') AS updated_log,
    ifNull(nullIf(JSONExtractString(k.payload, 'language_code'), ''), 'ko') AS language_code
FROM Data_R_Project_Mastodon_Log.kafka_webr_events_queue AS k
WHERE length(_error) = 0
  AND k.event_type = 'webr.mastodon.board.v1';

ALTER TABLE Data_R_Project_Mastodon_Log.mv_kafka_webr_events_to_board
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Kafka webr.mastodon.board.v1 events to Data_R_Project_Mastodon_Service.board';
