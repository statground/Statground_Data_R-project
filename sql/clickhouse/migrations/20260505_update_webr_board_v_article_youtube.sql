CREATE OR REPLACE VIEW webr_board.v_article_youtube
ON CLUSTER statground_cluster
AS
SELECT
    a.source,
    a.uuid,
    a.user_uuid,
    a.user_nickname,
    a.user_role,
    a.category,
    a.category_uuid,
    a.category_url,
    a.category_url_sub,
    a.title,
    a.content,
    a.file_url,
    a.file_name,
    a.is_new,
    a.is_secret,
    a.created_at,
    a.updated_at,
    a.cnt_read,
    a.cnt_comment,
    a.url,
    a.sort_priority,
    a.language_code,
    y.uuid AS youtube_uuid,
    y.url AS youtube_url,
    y.title AS youtube_title,
    y.description AS youtube_description,
    y.thumbnail AS youtube_thumbnail,
    y.views AS youtube_views,
    y.likes AS youtube_likes,
    y.duration AS youtube_duration,
    if(y.duration IS NULL, NULL, y.duration / 60.0) AS youtube_duration_min,
    y.publish_date AS youtube_publish_date,
    y.created_at AS youtube_created_at,
    y.updated_at AS youtube_updated_at
FROM webr_board.v_article a
INNER JOIN Data_R_Youtube_Service.v_webr_official_youtube y
    ON y.uuid_article = a.uuid
   AND y.language_code = a.language_code
WHERE a.source = 'article'
  AND coalesce(y.active, 0) = 1;
