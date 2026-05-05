CREATE OR REPLACE VIEW Data_R_Project_Mastodon_Service.v_r_foundation_latest
ON CLUSTER statground_cluster
AS
WITH ranked AS
(
    SELECT
        r.*,
        row_number() OVER (
            PARTITION BY r.instance_host, r.account_id, r.status_id
            ORDER BY r.fetched_at DESC, r.ingested_at DESC, r.uuid DESC
        ) AS rn
    FROM Data_R_Project_Mastodon_Raw.raw AS r
    WHERE r.instance_host = 'fosstodon.org'
      AND r.account_acct = 'R_Foundation'
)
SELECT
    uuid,
    event_uuid,
    instance_host,
    account_acct,
    account_id,
    status_id,
    status_uri,
    status_url,
    status_created_at,
    status_edited_at,
    visibility,
    language,
    language_code,
    sensitive,
    spoiler_text,
    content_html,
    content_text,
    in_reply_to_id,
    in_reply_to_account_id,
    is_reblog,
    reblog_status_id,
    replies_count,
    reblogs_count,
    favourites_count,
    tags_json,
    mentions_json,
    emojis_json,
    media_attachments_json,
    card_json,
    poll_json,
    raw_status_json,
    payload_hash,
    image_count,
    image_base64_count,
    has_image_base64,
    fetched_at,
    created_at,
    ingested_at
FROM ranked
WHERE rn = 1
  AND active = 1;
