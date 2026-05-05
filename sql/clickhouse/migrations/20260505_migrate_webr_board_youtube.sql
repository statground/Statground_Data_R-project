-- Move legacy Web-R official YouTube metadata from webr_board.youtube into
-- the Data_R_Youtube Raw/Service split. Keep the legacy source table for
-- rollback/count comparison.
--
-- Run after Data_R_Youtube_README/001_data_r_youtube_core.sql. The target
-- tables should be empty before this migration is applied.

SELECT
    'webr_board_youtube' AS table_name,
    (SELECT count() FROM webr_board.youtube) AS legacy_rows,
    (SELECT count() FROM Data_R_Youtube_Service.r_youtube_video_current WHERE source_tag = 'web_r_official_youtube') AS split_rows;

DROP VIEW IF EXISTS Data_R_Youtube_Service.mv_video_snapshot_raw_to_current
ON CLUSTER statground_cluster;

INSERT INTO Data_R_Youtube_Raw.r_youtube_video_snapshot_raw
SELECT
    y.uuid AS uuid,
    toString(y.uuid) AS event_id,
    ifNull(nullIf(extract(ifNull(y.url, ''), '(?:v=|youtu\\.be/|shorts/|live/)([A-Za-z0-9_-]{6,})'), ''), toString(y.uuid)) AS youtube_video_id,
    '' AS youtube_channel_id,
    '[]' AS playlist_ids,
    ifNull(y.title, '') AS video_title,
    ifNull(y.description, '') AS video_description,
    ifNull(y.url, '') AS canonical_url,
    ifNull(y.thumbnail, '') AS thumbnail_url,
    y.publish_date AS published_at,
    if(y.duration IS NULL OR y.duration < 0, NULL, toUInt32(y.duration)) AS duration_seconds,
    if(y.views IS NULL OR y.views < 0, NULL, toUInt64(y.views)) AS view_count,
    if(y.likes IS NULL OR y.likes < 0, NULL, toUInt64(y.likes)) AS like_count,
    CAST(NULL, 'Nullable(UInt64)') AS comment_count,
    CAST(NULL, 'Nullable(UInt64)') AS favorite_count,
    0 AS caption_available,
    '' AS default_audio_language,
    ifNull(y.language_code, 'ko') AS default_language,
    '[]' AS tags_json,
    '{}' AS thumbnail_urls_json,
    'legacy_webr_board_youtube' AS source_method,
    'web_r_official_youtube' AS source_tag,
    'web_r_board_article_video' AS source_category,
    'admin_migrated_legacy' AS source_confidence,
    ifNull(y.language_code, 'ko') AS language_code,
    y.uuid_article AS uuid_article,
    lower(hex(MD5(concat(toString(y.uuid), ':web_r_official_youtube')))) AS payload_hash,
    '' AS payload_json,
    ifNull(ifNull(y.updated_at, y.created_at), now64(3, 'Asia/Seoul')) AS observed_at,
    ifNull(ifNull(y.updated_at, y.created_at), now64(3, 'Asia/Seoul')) AS collected_at,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM webr_board.youtube y
WHERE ifNull(y.language_code, 'ko') != '';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Youtube_Service`.mv_video_snapshot_raw_to_current
ON CLUSTER statground_cluster
TO `Data_R_Youtube_Service`.r_youtube_video_current_local
AS
SELECT
    uuid,
    youtube_video_id,
    uuid_article,
    canonical_url,
    video_title,
    video_description,
    thumbnail_url,
    youtube_channel_id,
    JSONExtractString(payload_json, 'channel_title') AS channel_title,
    published_at,
    duration_seconds,
    ifNull(view_count, 0) AS view_count,
    ifNull(like_count, 0) AS like_count,
    ifNull(comment_count, 0) AS comment_count,
    caption_available,
    default_audio_language,
    default_language,
    tags_json,
    source_method,
    source_tag,
    source_category,
    source_confidence,
    language_code,
    1 AS active,
    payload_json,
    collected_at AS created_at,
    collected_at AS updated_at,
    collected_at,
    ingested_at
FROM `Data_R_Youtube_Raw`.r_youtube_video_snapshot_raw_local;

INSERT INTO Data_R_Youtube_Service.r_youtube_video_current
SELECT
    y.uuid AS uuid,
    ifNull(nullIf(extract(ifNull(y.url, ''), '(?:v=|youtu\\.be/|shorts/|live/)([A-Za-z0-9_-]{6,})'), ''), toString(y.uuid)) AS youtube_video_id,
    y.uuid_article AS uuid_article,
    ifNull(y.url, '') AS canonical_url,
    ifNull(y.title, '') AS video_title,
    ifNull(y.description, '') AS video_description,
    ifNull(y.thumbnail, '') AS thumbnail_url,
    '' AS youtube_channel_id,
    'Web-R official YouTube' AS channel_title,
    y.publish_date AS published_at,
    if(y.duration IS NULL OR y.duration < 0, NULL, toUInt32(y.duration)) AS duration_seconds,
    if(y.views IS NULL OR y.views < 0, 0, toUInt64(y.views)) AS view_count,
    if(y.likes IS NULL OR y.likes < 0, 0, toUInt64(y.likes)) AS like_count,
    0 AS comment_count,
    0 AS caption_available,
    '' AS default_audio_language,
    ifNull(y.language_code, 'ko') AS default_language,
    '[]' AS tags_json,
    'legacy_webr_board_youtube' AS source_method,
    'web_r_official_youtube' AS source_tag,
    'web_r_board_article_video' AS source_category,
    'admin_migrated_legacy' AS source_confidence,
    ifNull(y.language_code, 'ko') AS language_code,
    ifNull(y.active, 0) AS active,
    '' AS payload_json,
    ifNull(y.created_at, now64(3, 'Asia/Seoul')) AS created_at,
    ifNull(ifNull(y.updated_at, y.created_at), now64(3, 'Asia/Seoul')) AS updated_at,
    ifNull(ifNull(y.updated_at, y.created_at), now64(3, 'Asia/Seoul')) AS collected_at,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM webr_board.youtube y
WHERE ifNull(y.language_code, 'ko') != '';

SELECT
    'webr_board_youtube' AS table_name,
    (SELECT count() FROM webr_board.youtube) AS legacy_rows,
    (SELECT count() FROM Data_R_Youtube_Raw.r_youtube_video_snapshot_raw WHERE source_tag = 'web_r_official_youtube') AS raw_rows,
    (SELECT count() FROM Data_R_Youtube_Service.r_youtube_video_current WHERE source_tag = 'web_r_official_youtube') AS service_rows;
