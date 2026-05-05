# Data_R_Youtube Trino Queries

`Data_R_Youtube_*` is exposed through the ClickHouse Trino catalog for
cross-domain reports with `Data_R_Package_*`.

Example: package downloads and YouTube presence.

```sql
SELECT
    p.package_name,
    p.downloads_30d,
    y.video_count_30d,
    y.transcript_mention_count_total,
    y.youtube_presence_score
FROM clickhouse.Data_R_Package_Mart.package_profile_daily p
LEFT JOIN clickhouse.Data_R_Youtube_Mart.package_youtube_presence_daily y
    ON p.package_name = y.package_name
   AND p.report_date = y.report_date
WHERE p.report_date = current_date
ORDER BY y.youtube_presence_score DESC
LIMIT 100;
```

Example: Korean R content gap.

```sql
SELECT
    p.package_name,
    p.downloads_365d,
    p.reverse_imports_count,
    y.korean_video_count,
    y.content_gap_score_ko
FROM clickhouse.Data_R_Package_Mart.package_profile_daily p
LEFT JOIN clickhouse.Data_R_Youtube_Mart.package_youtube_presence_daily y
    ON p.package_name = y.package_name
   AND p.report_date = y.report_date
WHERE p.report_date = current_date
  AND p.downloads_365d >= 100000
ORDER BY y.content_gap_score_ko DESC
LIMIT 100;
```
