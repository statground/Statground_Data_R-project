CREATE OR REPLACE VIEW Data_R_Blogger_Service.v_rblogger
ON CLUSTER statground_cluster
AS
WITH
base AS
(
    SELECT
        r.uuid,
        r.url,
        r.created_at,
        r.updated_at,
        r.active,
        r.github_path,
        r.title,
        r.content,
        r.created_log,
        r.updated_log,
        r.language_code
    FROM Data_R_Blogger_Raw.raw AS r
    WHERE coalesce(r.active, toUInt8(0)) = toUInt8(1)
      AND match(ifNull(r.url, ''), '^https://www\\.r-bloggers\\.com/[0-9]{4}/[0-9]{2}/')
),
latest AS
(
    SELECT
        url,
        max(created_at) AS max_created_at
    FROM base
    GROUP BY url
),
picked AS
(
    SELECT b.*
    FROM base AS b
    INNER JOIN latest AS l
        ON b.url = l.url
       AND b.created_at = l.max_created_at
),
calc AS
(
    SELECT
        p.*,
        if(
            match(ifNull(p.url, ''), '/[0-9]{4}/[0-9]{2}/'),
            concat(
                extract(ifNull(p.url, ''), '/([0-9]{4})/'),
                '-',
                extract(ifNull(p.url, ''), '/[0-9]{4}/([0-9]{2})/')
            ),
            formatDateTime(p.created_at, '%Y-%m')
        ) AS ym,
        formatDateTime(p.created_at, '%d') AS dd,
        leftPad(toString(intDiv(CRC32(formatDateTime(p.created_at, '%Y-%m-%d %H:%i:%S')), 28) % 24), 2, '0') AS hh,
        leftPad(toString(intDiv(CRC32(formatDateTime(p.created_at, '%Y-%m-%d %H:%i:%S')), 28 * 24) % 60), 2, '0') AS mi,
        leftPad(toString(intDiv(CRC32(formatDateTime(p.created_at, '%Y-%m-%d %H:%i:%S')), 28 * 24 * 60) % 60), 2, '0') AS ss
    FROM picked AS p
)
SELECT
    c.uuid,
    c.url,
    c.created_at,
    c.updated_at,
    c.active,
    c.github_path,
    c.title,
    c.content,
    c.created_log,
    c.updated_log,
    c.language_code,
    concat(c.ym, '-', c.dd, 'T', c.hh, ':', c.mi, ':', c.ss, '+00:00') AS article_dt_utc_str,
    toDateTime(concat(c.ym, '-', c.dd, ' ', c.hh, ':', c.mi, ':', c.ss), 'UTC') AS article_dt_utc
FROM calc AS c;
