# Trino Data_R_Package Catalog Notes

The R package intelligence plan uses Trino as a read-only federation layer.

Expected catalog mapping:

- `clickhouse.Data_R_Package_Raw`
- `clickhouse.Data_R_Package_Log`
- `clickhouse.Data_R_Package_Service`
- `clickhouse.Data_R_Package_Mart`
- `clickhouse.Data_R_Youtube_Raw`
- `clickhouse.Data_R_Youtube_Log`
- `clickhouse.Data_R_Youtube_Service`
- `clickhouse.Data_R_Youtube_Mart`

The web application should use ClickHouse service/mart tables for repeated
dashboard reads. Trino joins are for administrator exploration and heavier
ad-hoc analysis across ClickHouse package, YouTube, Blogger, and Mastodon
analytics.
