CREATE OR REPLACE VIEW Data_R_Project_Mastodon_Service.v_r_foundation_board
ON CLUSTER statground_cluster
AS
WITH
raw_latest AS
(
    SELECT *
    FROM
    (
        SELECT
            r.*,
            row_number() OVER (
                PARTITION BY r.uuid, r.language_code
                ORDER BY r.fetched_at DESC, r.ingested_at DESC
            ) AS rn
        FROM Data_R_Project_Mastodon_Raw.raw AS r
        WHERE r.instance_host = 'fosstodon.org'
          AND r.account_acct = 'R_Foundation'
          AND r.language_code = 'en'
          AND r.active = 1
    )
    WHERE rn = 1
),
board_latest AS
(
    SELECT *
    FROM
    (
        SELECT
            b.*,
            row_number() OVER (
                PARTITION BY b.uuid, b.language_code
                ORDER BY b.version_at DESC, b.created_at DESC
            ) AS rn
        FROM Data_R_Project_Mastodon_Service.board AS b
        WHERE b.language_code = 'ko'
    )
    WHERE rn = 1
)
SELECT
    b.uuid,
    b.title,
    b.content,
    b.active,
    coalesce(r.status_created_at, b.created_at) AS created_at,
    b.updated_at,
    b.created_log,
    b.updated_log,
    b.language_code,
    r.status_id,
    r.status_url,
    r.status_created_at,
    r.content_text AS raw_content_text,
    r.language_code AS raw_language_code
FROM board_latest AS b
LEFT JOIN raw_latest AS r ON r.uuid = b.uuid
WHERE coalesce(b.active, 0) = 1;
