SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.cran_archive_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Source event UUID',
    event_id String COMMENT 'Source event_id string',
    repository LowCardinality(String) COMMENT 'Package repository, normally CRAN',
    package_name String COMMENT 'CRAN package name',
    archive_url String COMMENT 'CRAN Archive package directory URL',
    is_archived UInt8 COMMENT '1 when package appears in CRAN Archive index',
    observed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Upstream observation timestamp',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    payload_hash String COMMENT 'Payload SHA-256 hash',
    payload_json String COMMENT 'Full archive payload JSON string',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Raw/cran_archive_raw_local', '{replica}', collected_at)
PARTITION BY toYYYYMM(collected_at)
ORDER BY (repository, package_name, collected_at, uuid)
SETTINGS index_granularity = 8192
COMMENT 'CRAN archive/orphaned lifecycle raw local table. OLAP source for lifecycle reports.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.cran_archive_raw
ON CLUSTER statground_cluster
AS `Data_R_Package_Raw`.cran_archive_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Raw', 'cran_archive_raw_local', cityHash64(package_name))
COMMENT 'Distributed CRAN archive raw table.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Raw`.mv_rpkg_event_to_cran_archive
ON CLUSTER statground_cluster
TO `Data_R_Package_Raw`.cran_archive_raw_local
AS
SELECT
    uuid,
    event_id,
    repository,
    package_name,
    JSONExtractString(payload, 'archive_url') AS archive_url,
    toUInt8OrZero(JSONExtractRaw(payload, 'is_archived')) AS is_archived,
    observed_at,
    collected_at,
    payload_hash,
    payload AS payload_json,
    ingested_at
FROM `Data_R_Package_Raw`.r_package_event_raw_local
WHERE event_type = 'rpkg.cran.archive_snapshot.v1';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.cran_check_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Source event UUID',
    event_id String COMMENT 'Source event_id string',
    repository LowCardinality(String) COMMENT 'Package repository, normally CRAN',
    package_name String COMMENT 'CRAN package name',
    package_version String COMMENT 'Package version when known',
    flavor LowCardinality(String) COMMENT 'CRAN check flavor or summary',
    status LowCardinality(String) COMMENT 'OK, NOTE, WARNING, ERROR, or UNKNOWN',
    status_rank UInt8 COMMENT 'Severity rank: OK=1, NOTE=3, WARNING=4, ERROR=5',
    checked_at DateTime64(3, 'Asia/Seoul') COMMENT 'Check observation timestamp',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    raw_cells_json String COMMENT 'Raw parsed row cells JSON',
    payload_hash String COMMENT 'Payload SHA-256 hash',
    payload_json String COMMENT 'Full check payload JSON',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Raw/cran_check_raw_local', '{replica}', collected_at)
PARTITION BY toYYYYMM(collected_at)
ORDER BY (repository, package_name, flavor, collected_at, uuid)
SETTINGS index_granularity = 8192
COMMENT 'CRAN check snapshot raw local table. Flavor-level status signal for quality/risk reports.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.cran_check_raw
ON CLUSTER statground_cluster
AS `Data_R_Package_Raw`.cran_check_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Raw', 'cran_check_raw_local', cityHash64(package_name))
COMMENT 'Distributed CRAN check raw table.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Raw`.mv_rpkg_event_to_cran_check
ON CLUSTER statground_cluster
TO `Data_R_Package_Raw`.cran_check_raw_local
AS
SELECT
    uuid,
    event_id,
    repository,
    package_name,
    JSONExtractString(payload, 'version') AS package_version,
    JSONExtractString(payload, 'flavor') AS flavor,
    upper(JSONExtractString(payload, 'status')) AS status,
    multiIf(
        upper(JSONExtractString(payload, 'status')) = 'ERROR', 5,
        upper(JSONExtractString(payload, 'status')) = 'WARNING', 4,
        upper(JSONExtractString(payload, 'status')) = 'NOTE', 3,
        upper(JSONExtractString(payload, 'status')) = 'OK', 1,
        0
    ) AS status_rank,
    ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'checked_at'), 3, 'Asia/Seoul'), collected_at) AS checked_at,
    collected_at,
    JSONExtractRaw(payload, 'raw_cells') AS raw_cells_json,
    payload_hash,
    payload AS payload_json,
    ingested_at
FROM `Data_R_Package_Raw`.r_package_event_raw_local
WHERE event_type = 'rpkg.cran.check_snapshot.v1';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.package_dependency_edge_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Source event UUID',
    event_id String COMMENT 'Source event_id string',
    snapshot_date Date COMMENT 'Dependency graph snapshot date',
    source LowCardinality(String) COMMENT 'CRAN, Bioconductor, or R-universe',
    from_repository LowCardinality(String) COMMENT 'Repository of the depending package',
    from_package String COMMENT 'Depending package',
    from_version String COMMENT 'Depending package version',
    to_package String COMMENT 'Dependency target package',
    dependency_type LowCardinality(String) COMMENT 'Depends, Imports, Suggests, LinkingTo, or Enhances',
    dependency_spec String COMMENT 'Raw dependency spec including version constraint',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    payload_hash String COMMENT 'Payload SHA-256 hash',
    payload_json String COMMENT 'Full dependency payload JSON',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Raw/package_dependency_edge_raw_local', '{replica}', collected_at)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (snapshot_date, source, to_package, dependency_type, from_package, uuid)
SETTINGS index_granularity = 8192
COMMENT 'Package dependency edge raw local table. Enables reverse dependency and blast-radius reports.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.package_dependency_edge_raw
ON CLUSTER statground_cluster
AS `Data_R_Package_Raw`.package_dependency_edge_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Raw', 'package_dependency_edge_raw_local', cityHash64(to_package))
COMMENT 'Distributed package dependency edge raw table.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Raw`.mv_rpkg_event_to_dependency_edge
ON CLUSTER statground_cluster
TO `Data_R_Package_Raw`.package_dependency_edge_raw_local
AS
SELECT
    uuid,
    event_id,
    ifNull(toDateOrNull(JSONExtractString(payload, 'snapshot_date')), toDate(collected_at)) AS snapshot_date,
    JSONExtractString(payload, 'source') AS source,
    JSONExtractString(payload, 'from_repository') AS from_repository,
    JSONExtractString(payload, 'from_package') AS from_package,
    JSONExtractString(payload, 'from_version') AS from_version,
    JSONExtractString(payload, 'to_package') AS to_package,
    JSONExtractString(payload, 'dependency_type') AS dependency_type,
    JSONExtractString(payload, 'dependency_spec') AS dependency_spec,
    collected_at,
    payload_hash,
    payload AS payload_json,
    ingested_at
FROM `Data_R_Package_Raw`.r_package_event_raw_local
WHERE event_type = 'rpkg.cran.dependency_edge_snapshot.v1';

ALTER TABLE `Data_R_Package_Raw`.r_website_fetch_raw_local
ON CLUSTER statground_cluster
ADD COLUMN IF NOT EXISTS host String DEFAULT '' AFTER url_hash;

ALTER TABLE `Data_R_Package_Raw`.r_website_fetch_raw
ON CLUSTER statground_cluster
ADD COLUMN IF NOT EXISTS host String DEFAULT '' AFTER url_hash;

ALTER TABLE `Data_R_Package_Raw`.r_website_fetch_raw_local
ON CLUSTER statground_cluster
ADD COLUMN IF NOT EXISTS feed_urls_json String DEFAULT '' AFTER canonical_url;

ALTER TABLE `Data_R_Package_Raw`.r_website_fetch_raw
ON CLUSTER statground_cluster
ADD COLUMN IF NOT EXISTS feed_urls_json String DEFAULT '' AFTER canonical_url;

ALTER TABLE `Data_R_Package_Raw`.r_website_fetch_raw_local
ON CLUSTER statground_cluster
ADD COLUMN IF NOT EXISTS page_text String DEFAULT '' AFTER payload_json;

ALTER TABLE `Data_R_Package_Raw`.r_website_fetch_raw
ON CLUSTER statground_cluster
ADD COLUMN IF NOT EXISTS page_text String DEFAULT '' AFTER payload_json;

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Raw`.mv_rpkg_event_to_r_website_fetch
ON CLUSTER statground_cluster
TO `Data_R_Package_Raw`.r_website_fetch_raw_local
AS
SELECT
    uuid,
    toUUIDOrNull(JSONExtractString(payload, 'website_uuid')) AS uuid_website,
    JSONExtractString(payload, 'target_url') AS target_url,
    JSONExtractString(payload, 'url_hash') AS url_hash,
    JSONExtractString(payload, 'host') AS host,
    toUInt16OrZero(JSONExtractRaw(payload, 'status_code')) AS status_code,
    JSONExtractString(payload, 'content_type') AS content_type,
    JSONExtractString(payload, 'title') AS title,
    JSONExtractString(payload, 'description') AS description,
    JSONExtractString(payload, 'canonical_url') AS canonical_url,
    JSONExtractRaw(payload, 'feed_urls') AS feed_urls_json,
    JSONExtractString(payload, 'error_code') AS error_code,
    source,
    ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'fetched_at'), 3, 'Asia/Seoul'), collected_at) AS fetched_at,
    payload AS payload_json,
    JSONExtractString(payload, 'page_text') AS page_text,
    ingested_at
FROM `Data_R_Package_Raw`.r_package_event_raw_local
WHERE event_type = 'rpkg.r_website.fetch_snapshot.v1';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Raw`.mv_rpkg_event_to_r_website_package_mention
ON CLUSTER statground_cluster
TO `Data_R_Package_Raw`.r_website_package_mention_raw_local
AS
SELECT
    uuid,
    toUUIDOrNull(JSONExtractString(payload, 'website_uuid')) AS uuid_website,
    JSONExtractString(payload, 'source_url') AS source_url,
    JSONExtractString(payload, 'repository') AS repository,
    package_name,
    JSONExtractString(payload, 'mention_context') AS mention_context,
    toFloat64OrZero(JSONExtractRaw(payload, 'confidence')) AS confidence,
    ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'detected_at'), 3, 'Asia/Seoul'), collected_at) AS detected_at,
    source,
    payload AS payload_json
FROM `Data_R_Package_Raw`.r_package_event_raw_local
WHERE event_type = 'rpkg.r_website.package_mention_snapshot.v1';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Service`.package_version_history_local
ON CLUSTER statground_cluster
(
    repository LowCardinality(String),
    package_name String,
    package_version String,
    published_at Nullable(DateTime64(3, 'Asia/Seoul')),
    first_seen_at DateTime64(3, 'Asia/Seoul'),
    last_seen_at DateTime64(3, 'Asia/Seoul'),
    payload_hash String,
    metadata_json String,
    version UInt64
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Service/package_version_history_local', '{replica}', version)
ORDER BY (repository, package_name, package_version)
SETTINGS index_granularity = 8192
COMMENT 'Package version history service table derived from raw package snapshots.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Service`.package_version_history
ON CLUSTER statground_cluster
AS `Data_R_Package_Service`.package_version_history_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Service', 'package_version_history_local', cityHash64(package_name));

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Service`.mv_cran_package_snapshot_to_version_history
ON CLUSTER statground_cluster
TO `Data_R_Package_Service`.package_version_history_local
AS
SELECT
    repository,
    package_name,
    package_version,
    parseDateTime64BestEffortOrNull(date_publication, 3, 'Asia/Seoul') AS published_at,
    collected_at AS first_seen_at,
    collected_at AS last_seen_at,
    payload_hash,
    metadata_json,
    toUInt64(toUnixTimestamp64Milli(collected_at)) AS version
FROM `Data_R_Package_Raw`.cran_package_snapshot_raw_local;

CREATE TABLE IF NOT EXISTS `Data_R_Package_Service`.package_download_daily_local
ON CLUSTER statground_cluster
(
    repository LowCardinality(String),
    package_name String,
    download_date Date,
    downloads UInt64,
    period LowCardinality(String),
    source LowCardinality(String),
    collected_at DateTime64(3, 'Asia/Seoul'),
    version UInt64
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Service/package_download_daily_local', '{replica}', version)
PARTITION BY toYYYYMM(download_date)
ORDER BY (download_date, repository, package_name, period)
SETTINGS index_granularity = 8192
COMMENT 'Package download daily service table from cranlogs proxy raw events.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Service`.package_download_daily
ON CLUSTER statground_cluster
AS `Data_R_Package_Service`.package_download_daily_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Service', 'package_download_daily_local', cityHash64(package_name));

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Service`.mv_cran_download_daily_to_service
ON CLUSTER statground_cluster
TO `Data_R_Package_Service`.package_download_daily_local
AS
SELECT
    repository,
    package_name,
    download_date,
    downloads,
    period,
    source,
    collected_at,
    toUInt64(toUnixTimestamp64Milli(collected_at)) AS version
FROM `Data_R_Package_Raw`.cran_download_daily_raw_local;

CREATE TABLE IF NOT EXISTS `Data_R_Package_Service`.package_dependency_edge_current_local
ON CLUSTER statground_cluster
(
    snapshot_date Date,
    source LowCardinality(String),
    from_repository LowCardinality(String),
    from_package String,
    from_version String,
    to_package String,
    dependency_type LowCardinality(String),
    dependency_spec String,
    collected_at DateTime64(3, 'Asia/Seoul'),
    version UInt64
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Service/package_dependency_edge_current_local', '{replica}', version)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (snapshot_date, source, to_package, dependency_type, from_package)
SETTINGS index_granularity = 8192
COMMENT 'Current package dependency edges for reverse dependency reports.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Service`.package_dependency_edge_current
ON CLUSTER statground_cluster
AS `Data_R_Package_Service`.package_dependency_edge_current_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Service', 'package_dependency_edge_current_local', cityHash64(to_package));

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Service`.mv_dependency_edge_raw_to_current
ON CLUSTER statground_cluster
TO `Data_R_Package_Service`.package_dependency_edge_current_local
AS
SELECT
    snapshot_date,
    source,
    from_repository,
    from_package,
    from_version,
    to_package,
    dependency_type,
    dependency_spec,
    collected_at,
    toUInt64(toUnixTimestamp64Milli(collected_at)) AS version
FROM `Data_R_Package_Raw`.package_dependency_edge_raw_local;

CREATE TABLE IF NOT EXISTS `Data_R_Package_Service`.package_cran_check_current_local
ON CLUSTER statground_cluster
(
    repository LowCardinality(String),
    package_name String,
    package_version String,
    flavor LowCardinality(String),
    status LowCardinality(String),
    status_rank UInt8,
    checked_at DateTime64(3, 'Asia/Seoul'),
    collected_at DateTime64(3, 'Asia/Seoul'),
    raw_cells_json String,
    version UInt64
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Service/package_cran_check_current_local', '{replica}', version)
ORDER BY (repository, package_name, flavor)
SETTINGS index_granularity = 8192
COMMENT 'Current CRAN check status service table for package quality/risk views.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Service`.package_cran_check_current
ON CLUSTER statground_cluster
AS `Data_R_Package_Service`.package_cran_check_current_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Service', 'package_cran_check_current_local', cityHash64(package_name));

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Service`.mv_cran_check_raw_to_current
ON CLUSTER statground_cluster
TO `Data_R_Package_Service`.package_cran_check_current_local
AS
SELECT
    repository,
    package_name,
    package_version,
    flavor,
    status,
    status_rank,
    checked_at,
    collected_at,
    raw_cells_json,
    toUInt64(toUnixTimestamp64Milli(collected_at)) AS version
FROM `Data_R_Package_Raw`.cran_check_raw_local;

CREATE TABLE IF NOT EXISTS `Data_R_Package_Service`.package_lifecycle_current_local
ON CLUSTER statground_cluster
(
    repository LowCardinality(String),
    package_name String,
    is_archived UInt8,
    lifecycle_status LowCardinality(String),
    archive_url String,
    observed_at DateTime64(3, 'Asia/Seoul'),
    collected_at DateTime64(3, 'Asia/Seoul'),
    version UInt64
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Service/package_lifecycle_current_local', '{replica}', version)
ORDER BY (repository, package_name)
SETTINGS index_granularity = 8192
COMMENT 'Current package lifecycle service table for archived/orphaned/stale reports.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Service`.package_lifecycle_current
ON CLUSTER statground_cluster
AS `Data_R_Package_Service`.package_lifecycle_current_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Service', 'package_lifecycle_current_local', cityHash64(package_name));

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Service`.mv_cran_archive_raw_to_lifecycle_current
ON CLUSTER statground_cluster
TO `Data_R_Package_Service`.package_lifecycle_current_local
AS
SELECT
    repository,
    package_name,
    is_archived,
    if(is_archived = 1, 'archived', 'active') AS lifecycle_status,
    archive_url,
    observed_at,
    collected_at,
    toUInt64(toUnixTimestamp64Milli(collected_at)) AS version
FROM `Data_R_Package_Raw`.cran_archive_raw_local;

CREATE TABLE IF NOT EXISTS `Data_R_Package_Service`.r_website_current_local
ON CLUSTER statground_cluster
(
    url_hash String,
    target_url String,
    host String,
    title String,
    description String,
    canonical_url String,
    feed_urls_json String,
    status_code UInt16,
    last_success_at Nullable(DateTime64(3, 'Asia/Seoul')),
    last_fetch_at DateTime64(3, 'Asia/Seoul'),
    error_code String,
    version UInt64
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Service/r_website_current_local', '{replica}', version)
ORDER BY (host, url_hash)
SETTINGS index_granularity = 8192
COMMENT 'Current R website directory service table for the Web-R R ecosystem menu.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Service`.r_website_current
ON CLUSTER statground_cluster
AS `Data_R_Package_Service`.r_website_current_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Service', 'r_website_current_local', cityHash64(url_hash));

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Service`.mv_r_website_fetch_to_current
ON CLUSTER statground_cluster
TO `Data_R_Package_Service`.r_website_current_local
AS
SELECT
    url_hash,
    target_url,
    host,
    title,
    description,
    canonical_url,
    feed_urls_json,
    status_code,
    if(status_code >= 200 AND status_code < 400, fetched_at, CAST(NULL, 'Nullable(DateTime64(3, ''Asia/Seoul''))')) AS last_success_at,
    fetched_at AS last_fetch_at,
    error_code,
    toUInt64(toUnixTimestamp64Milli(fetched_at)) AS version
FROM `Data_R_Package_Raw`.r_website_fetch_raw_local;

CREATE OR REPLACE VIEW `Data_R_Package_Service`.v_package_reverse_dependency_daily
ON CLUSTER statground_cluster
AS
SELECT
    snapshot_date,
    'CRAN' AS repository,
    to_package AS package_name,
    countIf(dependency_type = 'Depends') AS reverse_depends_count,
    countIf(dependency_type = 'Imports') AS reverse_imports_count,
    countIf(dependency_type = 'Suggests') AS reverse_suggests_count,
    countIf(dependency_type = 'LinkingTo') AS reverse_linkingto_count,
    countIf(dependency_type = 'Enhances') AS reverse_enhances_count,
    count() AS direct_reverse_dependency_count
FROM `Data_R_Package_Service`.package_dependency_edge_current
GROUP BY
    snapshot_date,
    to_package;

ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS published_at Nullable(DateTime64(3, 'Asia/Seoul')) AFTER latest_version;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS published_at Nullable(DateTime64(3, 'Asia/Seoul')) AFTER latest_version;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS first_seen_at Nullable(DateTime64(3, 'Asia/Seoul')) AFTER published_at;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS first_seen_at Nullable(DateTime64(3, 'Asia/Seoul')) AFTER published_at;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS last_seen_at Nullable(DateTime64(3, 'Asia/Seoul')) AFTER first_seen_at;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS last_seen_at Nullable(DateTime64(3, 'Asia/Seoul')) AFTER first_seen_at;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS lifecycle_status LowCardinality(String) DEFAULT '' AFTER last_seen_at;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS lifecycle_status LowCardinality(String) DEFAULT '' AFTER last_seen_at;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS downloads_prev_30d UInt64 DEFAULT 0 AFTER downloads_30d;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS downloads_prev_30d UInt64 DEFAULT 0 AFTER downloads_30d;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS download_growth_30d Float64 DEFAULT 0 AFTER downloads_prev_30d;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS download_growth_30d Float64 DEFAULT 0 AFTER downloads_prev_30d;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS reverse_depends_count UInt64 DEFAULT 0 AFTER download_growth_30d;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS reverse_depends_count UInt64 DEFAULT 0 AFTER download_growth_30d;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS reverse_imports_count UInt64 DEFAULT 0 AFTER reverse_depends_count;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS reverse_imports_count UInt64 DEFAULT 0 AFTER reverse_depends_count;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS recursive_reverse_dep_count UInt64 DEFAULT 0 AFTER reverse_imports_count;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS recursive_reverse_dep_count UInt64 DEFAULT 0 AFTER reverse_imports_count;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS dependency_pagerank Float64 DEFAULT 0 AFTER recursive_reverse_dep_count;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS dependency_pagerank Float64 DEFAULT 0 AFTER recursive_reverse_dep_count;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS cran_check_worst_status LowCardinality(String) DEFAULT '' AFTER dependency_pagerank;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS cran_check_worst_status LowCardinality(String) DEFAULT '' AFTER dependency_pagerank;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS cran_check_error_count UInt64 DEFAULT 0 AFTER cran_check_worst_status;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS cran_check_error_count UInt64 DEFAULT 0 AFTER cran_check_worst_status;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS cran_check_warning_count UInt64 DEFAULT 0 AFTER cran_check_error_count;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS cran_check_warning_count UInt64 DEFAULT 0 AFTER cran_check_error_count;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS repo_url String DEFAULT '' AFTER cran_check_warning_count;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS repo_url String DEFAULT '' AFTER cran_check_warning_count;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS license_text String DEFAULT '' AFTER repo_url;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS license_text String DEFAULT '' AFTER repo_url;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS license_risk LowCardinality(String) DEFAULT '' AFTER license_text;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS license_risk LowCardinality(String) DEFAULT '' AFTER license_text;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS vulnerability_count UInt64 DEFAULT 0 AFTER license_risk;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS vulnerability_count UInt64 DEFAULT 0 AFTER license_risk;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS critical_vulnerability_count UInt64 DEFAULT 0 AFTER vulnerability_count;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS critical_vulnerability_count UInt64 DEFAULT 0 AFTER vulnerability_count;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS popularity_score Float64 DEFAULT 0 AFTER critical_vulnerability_count;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS popularity_score Float64 DEFAULT 0 AFTER critical_vulnerability_count;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily_local ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS adoption_score Float64 DEFAULT 0 AFTER popularity_score;
ALTER TABLE `Data_R_Package_Mart`.package_profile_daily ON CLUSTER statground_cluster ADD COLUMN IF NOT EXISTS adoption_score Float64 DEFAULT 0 AFTER popularity_score;

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.package_score_daily_local
ON CLUSTER statground_cluster
(
    report_date Date,
    repository LowCardinality(String),
    package_name String,
    popularity_score Float64,
    adoption_score Float64,
    maintenance_score Float64,
    quality_score Float64,
    risk_score Float64,
    overall_score Float64,
    score_json String,
    computed_at DateTime64(3, 'Asia/Seoul')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Mart/package_score_daily_local', '{replica}', computed_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, repository, package_name)
SETTINGS index_granularity = 8192
COMMENT 'Daily score breakdown mart for package cards and recommendation surfaces.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.package_score_daily
ON CLUSTER statground_cluster
AS `Data_R_Package_Mart`.package_score_daily_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Mart', 'package_score_daily_local', cityHash64(package_name));

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.ecosystem_summary_daily_local
ON CLUSTER statground_cluster
(
    report_date Date,
    repository LowCardinality(String),
    total_packages UInt64,
    new_packages UInt64,
    updated_packages UInt64,
    archived_packages UInt64,
    check_error_packages UInt64,
    check_warning_packages UInt64,
    total_downloads_30d UInt64,
    report_json String,
    computed_at DateTime64(3, 'Asia/Seoul')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Mart/ecosystem_summary_daily_local', '{replica}', computed_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, repository)
SETTINGS index_granularity = 8192
COMMENT 'Daily R package ecosystem summary mart for the R-Project dashboard.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.ecosystem_summary_daily
ON CLUSTER statground_cluster
AS `Data_R_Package_Mart`.ecosystem_summary_daily_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Mart', 'ecosystem_summary_daily_local', cityHash64(repository));

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.topic_summary_weekly_local
ON CLUSTER statground_cluster
(
    week_start Date,
    topic_code LowCardinality(String),
    repository LowCardinality(String),
    package_count UInt64,
    downloads_30d UInt64,
    growth_score Float64,
    risk_package_count UInt64,
    report_json String,
    computed_at DateTime64(3, 'Asia/Seoul')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Mart/topic_summary_weekly_local', '{replica}', computed_at)
PARTITION BY toYYYYMM(week_start)
ORDER BY (week_start, topic_code, repository)
SETTINGS index_granularity = 8192
COMMENT 'Weekly topic/domain trend mart seeded by CRAN Task Views, BiocViews, R-universe topics, and text clustering.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.topic_summary_weekly
ON CLUSTER statground_cluster
AS `Data_R_Package_Mart`.topic_summary_weekly_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Mart', 'topic_summary_weekly_local', cityHash64(topic_code));

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.package_comparison_local
ON CLUSTER statground_cluster
(
    comparison_uuid UUID,
    comparison_topic String,
    package_name String,
    repository LowCardinality(String),
    rank_order UInt32,
    recommendation_class LowCardinality(String),
    comparison_json String,
    computed_at DateTime64(3, 'Asia/Seoul')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Mart/package_comparison_local', '{replica}', computed_at)
ORDER BY (comparison_topic, rank_order, repository, package_name)
SETTINGS index_granularity = 8192
COMMENT 'Package alternative/comparison mart for Web-R package selection reports.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.package_comparison
ON CLUSTER statground_cluster
AS `Data_R_Package_Mart`.package_comparison_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Mart', 'package_comparison_local', cityHash64(comparison_topic));

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.enterprise_allowlist_local
ON CLUSTER statground_cluster
(
    policy_date Date,
    repository LowCardinality(String),
    package_name String,
    policy_status LowCardinality(String),
    severity_code LowCardinality(String),
    reason_json String,
    recommendation String,
    computed_at DateTime64(3, 'Asia/Seoul')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Mart/enterprise_allowlist_local', '{replica}', computed_at)
PARTITION BY toYYYYMM(policy_date)
ORDER BY (policy_date, policy_status, repository, package_name)
SETTINGS index_granularity = 8192
COMMENT 'Enterprise allowlist/review/blocklist mart for production R package policy.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.enterprise_allowlist
ON CLUSTER statground_cluster
AS `Data_R_Package_Mart`.enterprise_allowlist_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Mart', 'enterprise_allowlist_local', cityHash64(package_name));

CREATE OR REPLACE VIEW `Data_R_Package_Mart`.v_package_profile_latest
ON CLUSTER statground_cluster
AS
SELECT
    today() AS report_date,
    pc.repository AS repository,
    pc.package_name AS package_name,
    pc.latest_version,
    pc.title,
    pc.description,
    pc.maintainer,
    pc.license_text,
    pc.last_observed_at,
    ifNull(dl.downloads_30d, 0) AS downloads_30d,
    ifNull(dep.reverse_depends_count, 0) AS reverse_depends_count,
    ifNull(dep.reverse_imports_count, 0) AS reverse_imports_count,
    ifNull(ch.worst_status, '') AS cran_check_worst_status,
    ifNull(lc.lifecycle_status, 'active') AS lifecycle_status
FROM `Data_R_Package_Service`.package_current AS pc
LEFT JOIN
(
    SELECT
        repository,
        package_name,
        sumIf(downloads, download_date >= today() - 30) AS downloads_30d
    FROM `Data_R_Package_Service`.package_download_daily
    GROUP BY repository, package_name
) AS dl USING (repository, package_name)
LEFT JOIN
(
    SELECT
        repository,
        package_name,
        sum(reverse_depends_count) AS reverse_depends_count,
        sum(reverse_imports_count) AS reverse_imports_count
    FROM `Data_R_Package_Service`.v_package_reverse_dependency_daily
    GROUP BY repository, package_name
) AS dep USING (repository, package_name)
LEFT JOIN
(
    SELECT
        repository,
        package_name,
        argMax(status, status_rank) AS worst_status
    FROM `Data_R_Package_Service`.package_cran_check_current
    GROUP BY repository, package_name
) AS ch USING (repository, package_name)
LEFT JOIN `Data_R_Package_Service`.package_lifecycle_current AS lc USING (repository, package_name);

CREATE OR REPLACE VIEW `Data_R_Package_Mart`.v_ecosystem_summary_today
ON CLUSTER statground_cluster
AS
SELECT
    today() AS report_date,
    repository,
    countDistinct(package_name) AS total_packages,
    countIf(lifecycle_status = 'archived') AS archived_packages,
    countIf(cran_check_worst_status = 'ERROR') AS check_error_packages,
    countIf(cran_check_worst_status = 'WARNING') AS check_warning_packages,
    sum(downloads_30d) AS total_downloads_30d
FROM `Data_R_Package_Mart`.v_package_profile_latest
GROUP BY repository;
