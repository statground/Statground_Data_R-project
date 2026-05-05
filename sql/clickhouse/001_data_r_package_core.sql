SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

CREATE DATABASE IF NOT EXISTS `Data_R_Package_Raw`
ON CLUSTER statground_cluster
COMMENT 'R package raw collection events. ClickHouse OLAP only; ClickHouse owns collector operational data through Data_R_Package_Log and Data_R_Package_Service.';

CREATE DATABASE IF NOT EXISTS `Data_R_Package_Log`
ON CLUSTER statground_cluster
COMMENT 'R package Kafka queues and ingestion logs. ClickHouse OLAP operational log layer.';

CREATE DATABASE IF NOT EXISTS `Data_R_Package_Service`
ON CLUSTER statground_cluster
COMMENT 'R package normalized service tables. ClickHouse OLAP serving layer; ClickHouse owns collector operational data through Data_R_Package_Log and Data_R_Package_Service.';

CREATE DATABASE IF NOT EXISTS `Data_R_Package_Mart`
ON CLUSTER statground_cluster
COMMENT 'R package report and dashboard marts. ClickHouse OLAP only.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.r_package_event_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Event UUID v7 converted from Kafka event_id',
    event_id String COMMENT 'Kafka event_id as published by collector',
    event_type LowCardinality(String) COMMENT 'Event type discriminator such as rpkg.cran.package_snapshot.v1',
    schema_version UInt16 COMMENT 'Event schema version',
    source LowCardinality(String) COMMENT 'Collector source code',
    source_url String COMMENT 'Source URL that produced this event',
    repository LowCardinality(String) COMMENT 'Package repository such as CRAN, Bioconductor, R-universe, or R-Core',
    package_name String COMMENT 'Package name; empty for ecosystem-wide events',
    package_version String COMMENT 'Package version when applicable',
    observed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Upstream observation timestamp normalized for analytic queries',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp normalized for analytic queries',
    payload_hash String COMMENT 'SHA-256 hash of canonical payload JSON',
    payload String COMMENT 'Canonical payload JSON string; raw audit field; SSOT not here',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Raw/r_package_event_raw_local', '{replica}')
PARTITION BY toYYYYMM(collected_at)
ORDER BY (collected_at, event_type, repository, package_name, uuid)
SETTINGS index_granularity = 8192
COMMENT 'Generic R package collector event raw local table. Kafka-fed OLAP audit data; SSOT not here.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.r_package_event_raw
ON CLUSTER statground_cluster
AS `Data_R_Package_Raw`.r_package_event_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Raw', 'r_package_event_raw_local', cityHash64(event_id))
COMMENT 'Distributed generic R package collector event raw table.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Log`.kafka_r_package_event_queue
ON CLUSTER statground_cluster
(
    event_id String COMMENT 'Kafka JSON event_id. UUID v7 string',
    event_type String COMMENT 'Kafka JSON event_type',
    schema_version UInt16 COMMENT 'Kafka JSON schema_version',
    source String COMMENT 'Kafka JSON source',
    source_url String COMMENT 'Kafka JSON source_url',
    repository String COMMENT 'Kafka JSON repository',
    package_name String COMMENT 'Kafka JSON package_name',
    package_version String COMMENT 'Kafka JSON package_version',
    observed_at String COMMENT 'Kafka JSON observed_at ISO-8601 string',
    collected_at String COMMENT 'Kafka JSON collected_at ISO-8601 string',
    payload_hash String COMMENT 'Kafka JSON payload_hash SHA-256 hex string',
    payload String COMMENT 'Kafka JSON payload string'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-platform:19092',
    kafka_topic_list = 'rpkg.events',
    kafka_group_name = 'clickhouse_data_r_package_events_v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream'
COMMENT 'Kafka Engine queue for rpkg.events. Do not SELECT except for debugging.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Log`.kafka_ingest_error_local
ON CLUSTER statground_cluster
(
    event_id_raw String COMMENT 'Raw event_id string from Kafka message if parsed; may be empty for parser errors',
    kafka_topic LowCardinality(String) COMMENT 'Kafka topic where malformed or invalid message was consumed',
    kafka_partition UInt32 COMMENT 'Kafka partition of malformed or invalid message',
    kafka_offset UInt64 COMMENT 'Kafka offset of malformed or invalid message',
    event_type LowCardinality(String) COMMENT 'Event type if parsed; empty when JSON parsing failed',
    raw_message String COMMENT 'Original raw Kafka message for parser errors; replay/debug only',
    error_message String COMMENT 'ClickHouse parser error message',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse error capture timestamp'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Log/kafka_ingest_error_local', '{replica}')
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ingested_at, kafka_topic, kafka_partition, kafka_offset)
SETTINGS index_granularity = 8192
COMMENT 'R package Kafka ingestion parser error local replicated table. OLAP log only.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Log`.kafka_ingest_error
ON CLUSTER statground_cluster
AS `Data_R_Package_Log`.kafka_ingest_error_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Log', 'kafka_ingest_error_local', rand())
COMMENT 'Distributed R package Kafka ingestion parser error table.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Log`.mv_kafka_r_package_event_to_raw
ON CLUSTER statground_cluster
TO `Data_R_Package_Raw`.r_package_event_raw_local
AS
SELECT
    ifNull(toUUIDOrNull(event_id), generateUUIDv4()) AS uuid,
    event_id,
    event_type,
    schema_version,
    source,
    source_url,
    repository,
    package_name,
    package_version,
    ifNull(parseDateTime64BestEffortOrNull(observed_at, 3, 'Asia/Seoul'), now64(3, 'Asia/Seoul')) AS observed_at,
    ifNull(parseDateTime64BestEffortOrNull(collected_at, 3, 'Asia/Seoul'), now64(3, 'Asia/Seoul')) AS collected_at,
    payload_hash,
    payload,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM `Data_R_Package_Log`.kafka_r_package_event_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Log`.mv_kafka_r_package_event_parse_error_to_dlq
ON CLUSTER statground_cluster
TO `Data_R_Package_Log`.kafka_ingest_error_local
AS
SELECT
    event_id AS event_id_raw,
    _topic AS kafka_topic,
    toUInt32(_partition) AS kafka_partition,
    toUInt64(_offset) AS kafka_offset,
    event_type AS event_type,
    _raw_message AS raw_message,
    _error AS error_message,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM `Data_R_Package_Log`.kafka_r_package_event_queue
WHERE length(_error) > 0;

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.cran_package_snapshot_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Source event UUID',
    event_id String COMMENT 'Source event_id string',
    repository LowCardinality(String) COMMENT 'Package repository, normally CRAN',
    package_name String COMMENT 'CRAN package name',
    package_version String COMMENT 'CRAN package version',
    title String COMMENT 'CRAN package title',
    description String COMMENT 'CRAN package description',
    maintainer String COMMENT 'CRAN maintainer string',
    license_text String COMMENT 'CRAN license string',
    depends String COMMENT 'DESCRIPTION Depends field',
    imports String COMMENT 'DESCRIPTION Imports field',
    suggests String COMMENT 'DESCRIPTION Suggests field',
    linking_to String COMMENT 'DESCRIPTION LinkingTo field',
    needs_compilation LowCardinality(String) COMMENT 'DESCRIPTION NeedsCompilation field',
    date_publication String COMMENT 'DESCRIPTION Date/Publication field as raw text',
    metadata_json String COMMENT 'Full normalized payload JSON string',
    source_url String COMMENT 'Source URL',
    observed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Upstream observation timestamp',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    payload_hash String COMMENT 'Payload SHA-256 hash',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Raw/cran_package_snapshot_raw_local', '{replica}', collected_at)
PARTITION BY toYYYYMM(collected_at)
ORDER BY (repository, package_name, package_version, collected_at, uuid)
SETTINGS index_granularity = 8192
COMMENT 'CRAN DESCRIPTION package snapshot raw local table. OLAP only; package identity is projected into Data_R_Package_Service.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.cran_package_snapshot_raw
ON CLUSTER statground_cluster
AS `Data_R_Package_Raw`.cran_package_snapshot_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Raw', 'cran_package_snapshot_raw_local', cityHash64(package_name))
COMMENT 'Distributed CRAN package snapshot raw table.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Raw`.mv_rpkg_event_to_cran_package_snapshot
ON CLUSTER statground_cluster
TO `Data_R_Package_Raw`.cran_package_snapshot_raw_local
AS
SELECT
    uuid,
    event_id,
    repository,
    package_name,
    package_version,
    JSONExtractString(payload, 'title') AS title,
    JSONExtractString(payload, 'description') AS description,
    JSONExtractString(payload, 'maintainer') AS maintainer,
    JSONExtractString(payload, 'license') AS license_text,
    JSONExtractString(payload, 'depends') AS depends,
    JSONExtractString(payload, 'imports') AS imports,
    JSONExtractString(payload, 'suggests') AS suggests,
    JSONExtractString(payload, 'linking_to') AS linking_to,
    JSONExtractString(payload, 'needs_compilation') AS needs_compilation,
    JSONExtractString(payload, 'date_publication') AS date_publication,
    payload AS metadata_json,
    source_url,
    observed_at,
    collected_at,
    payload_hash,
    ingested_at
FROM `Data_R_Package_Raw`.r_package_event_raw_local
WHERE event_type = 'rpkg.cran.package_snapshot.v1';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.cran_download_daily_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Source event UUID',
    event_id String COMMENT 'Source event_id string',
    repository LowCardinality(String) COMMENT 'Package repository, normally CRAN',
    package_name String COMMENT 'CRAN package name',
    download_date Date COMMENT 'Download date from cranlogs',
    downloads UInt64 COMMENT 'Download count from cranlogs proxy',
    period LowCardinality(String) COMMENT 'cranlogs period used by collector',
    source LowCardinality(String) COMMENT 'Metric source, normally cranlogs',
    source_url String COMMENT 'Source URL',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    payload_hash String COMMENT 'Payload SHA-256 hash',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Raw/cran_download_daily_raw_local', '{replica}', collected_at)
PARTITION BY toYYYYMM(download_date)
ORDER BY (download_date, repository, package_name, uuid)
SETTINGS index_granularity = 8192
COMMENT 'CRAN daily download raw table from cranlogs proxy. OLAP trend metric; absolute totals are not CRAN-wide truth.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.cran_download_daily_raw
ON CLUSTER statground_cluster
AS `Data_R_Package_Raw`.cran_download_daily_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Raw', 'cran_download_daily_raw_local', cityHash64(package_name))
COMMENT 'Distributed CRAN daily download raw table.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Raw`.mv_rpkg_event_to_cran_download_daily
ON CLUSTER statground_cluster
TO `Data_R_Package_Raw`.cran_download_daily_raw_local
AS
SELECT
    uuid,
    event_id,
    repository,
    package_name,
    ifNull(toDateOrNull(JSONExtractString(payload, 'download_date')), toDate('1970-01-01')) AS download_date,
    toUInt64OrZero(JSONExtractRaw(payload, 'downloads')) AS downloads,
    JSONExtractString(payload, 'period') AS period,
    JSONExtractString(payload, 'source') AS source,
    source_url,
    collected_at,
    payload_hash,
    ingested_at
FROM `Data_R_Package_Raw`.r_package_event_raw_local
WHERE event_type = 'rpkg.cran.download_daily.v1';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.r_core_release_note_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Source event UUID',
    event_id String COMMENT 'Source event_id string',
    title String COMMENT 'R Core NEWS page title',
    headings_json String COMMENT 'Recent release headings JSON array',
    source_url String COMMENT 'R Core NEWS URL',
    observed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Observation timestamp',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    payload_hash String COMMENT 'Payload SHA-256 hash',
    payload String COMMENT 'Full payload JSON',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Raw/r_core_release_note_raw_local', '{replica}', collected_at)
PARTITION BY toYYYYMM(collected_at)
ORDER BY (collected_at, uuid)
SETTINGS index_granularity = 8192
COMMENT 'R Core NEWS snapshot raw local table. OLAP release-impact input.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.r_core_release_note_raw
ON CLUSTER statground_cluster
AS `Data_R_Package_Raw`.r_core_release_note_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Raw', 'r_core_release_note_raw_local', cityHash64(event_id))
COMMENT 'Distributed R Core NEWS snapshot raw table.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Raw`.mv_rpkg_event_to_r_core_release_note
ON CLUSTER statground_cluster
TO `Data_R_Package_Raw`.r_core_release_note_raw_local
AS
SELECT
    uuid,
    event_id,
    JSONExtractString(payload, 'title') AS title,
    JSONExtractRaw(payload, 'headings') AS headings_json,
    source_url,
    observed_at,
    collected_at,
    payload_hash,
    payload,
    ingested_at
FROM `Data_R_Package_Raw`.r_package_event_raw_local
WHERE event_type = 'rpkg.r_core.news_snapshot.v1';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Service`.package_current_local
ON CLUSTER statground_cluster
(
    repository LowCardinality(String) COMMENT 'Package repository such as CRAN',
    package_name String COMMENT 'Package name',
    latest_version String COMMENT 'Latest observed version',
    title String COMMENT 'Latest observed title',
    description String COMMENT 'Latest observed description',
    maintainer String COMMENT 'Latest observed maintainer',
    license_text String COMMENT 'Latest observed license',
    date_publication String COMMENT 'Latest raw Date/Publication text',
    metadata_json String COMMENT 'Latest normalized source payload JSON',
    last_observed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Last collector observation timestamp',
    version UInt64 COMMENT 'ReplacingMergeTree version from last_observed_at milliseconds'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Service/package_current_local', '{replica}', version)
ORDER BY (repository, package_name)
SETTINGS index_granularity = 8192
COMMENT 'Latest R package profile service table. Denormalized for web_r_go/API reads; Data_R_Package_Service is the serving identity projection.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Service`.package_current
ON CLUSTER statground_cluster
AS `Data_R_Package_Service`.package_current_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Service', 'package_current_local', cityHash64(package_name))
COMMENT 'Distributed latest R package profile service table.';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_R_Package_Service`.mv_cran_package_snapshot_to_package_current
ON CLUSTER statground_cluster
TO `Data_R_Package_Service`.package_current_local
AS
SELECT
    repository,
    package_name,
    package_version AS latest_version,
    title,
    description,
    maintainer,
    license_text,
    date_publication,
    metadata_json,
    collected_at AS last_observed_at,
    toUInt64(toUnixTimestamp64Milli(collected_at)) AS version
FROM `Data_R_Package_Raw`.cran_package_snapshot_raw_local;

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.package_profile_daily_local
ON CLUSTER statground_cluster
(
    report_date Date COMMENT 'Report date',
    repository LowCardinality(String) COMMENT 'Package repository',
    package_name String COMMENT 'Package name',
    latest_version String COMMENT 'Latest version as of report date',
    downloads_1d UInt64 COMMENT 'Downloads during report date',
    downloads_7d UInt64 COMMENT 'Downloads during trailing seven days',
    downloads_30d UInt64 COMMENT 'Downloads during trailing thirty days',
    maintenance_score Float64 COMMENT 'Maintenance score 0-100',
    quality_score Float64 COMMENT 'Quality score 0-100',
    risk_score Float64 COMMENT 'Risk score 0-100 where higher means more risk',
    overall_score Float64 COMMENT 'Overall score 0-100',
    report_json String COMMENT 'Additional report JSON',
    computed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Mart computation timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Mart/package_profile_daily_local', '{replica}', computed_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, repository, package_name)
SETTINGS index_granularity = 8192
COMMENT 'Daily R package profile mart for dashboard/reporting. OLAP only.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.package_profile_daily
ON CLUSTER statground_cluster
AS `Data_R_Package_Mart`.package_profile_daily_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Mart', 'package_profile_daily_local', cityHash64(package_name))
COMMENT 'Distributed daily R package profile mart.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.r_release_impact_daily_local
ON CLUSTER statground_cluster
(
    report_date Date COMMENT 'Report date',
    r_version String COMMENT 'R version label',
    category_code LowCardinality(String) COMMENT 'Release note category',
    impact_count UInt64 COMMENT 'Number of detected impact signals',
    packages_affected UInt64 COMMENT 'Estimated affected package count',
    summary String COMMENT 'Human-readable summary',
    computed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Mart computation timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Mart/r_release_impact_daily_local', '{replica}', computed_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, r_version, category_code)
SETTINGS index_granularity = 8192
COMMENT 'Daily R Core release impact mart. OLAP only.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.r_release_impact_daily
ON CLUSTER statground_cluster
AS `Data_R_Package_Mart`.r_release_impact_daily_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Mart', 'r_release_impact_daily_local', cityHash64(r_version))
COMMENT 'Distributed daily R Core release impact mart.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.package_alert_event_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Alert event UUID v7; alert workflow projections stay in ClickHouse service/mart layers',
    detected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Detection timestamp',
    severity_code LowCardinality(String) COMMENT 'critical, high, medium, or low',
    repository LowCardinality(String) COMMENT 'Package repository or empty for ecosystem alert',
    package_name String COMMENT 'Package name or empty for ecosystem alert',
    alert_type LowCardinality(String) COMMENT 'Alert type discriminator',
    message String COMMENT 'Alert message',
    alert_json String COMMENT 'Additional alert payload JSON'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Mart/package_alert_event_local', '{replica}')
PARTITION BY toYYYYMM(detected_at)
ORDER BY (detected_at, severity_code, repository, package_name, uuid)
SETTINGS index_granularity = 8192
COMMENT 'R package alert event mart. ClickHouse stores detected events and downstream workflow projections.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.package_alert_event
ON CLUSTER statground_cluster
AS `Data_R_Package_Mart`.package_alert_event_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Mart', 'package_alert_event_local', cityHash64(toString(uuid)))
COMMENT 'Distributed R package alert event mart.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.r_website_fetch_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Fetch event UUID v7',
    uuid_website Nullable(UUID) COMMENT 'R website UUID when known',
    target_url String COMMENT 'Fetched URL',
    url_hash String COMMENT 'SHA-256 hex hash of canonical target URL',
    status_code UInt16 COMMENT 'HTTP status code; 0 for transport failure',
    content_type String COMMENT 'HTTP Content-Type',
    title String COMMENT 'Extracted page title',
    description String COMMENT 'Extracted meta description or summary',
    canonical_url String COMMENT 'Canonical URL detected by collector',
    error_code LowCardinality(String) COMMENT 'Collector error code; empty when success',
    source LowCardinality(String) COMMENT 'Collector source name',
    fetched_at DateTime64(3, 'Asia/Seoul') COMMENT 'Fetch timestamp',
    payload_json String COMMENT 'Fetch metadata payload JSON',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Raw/r_website_fetch_raw_local', '{replica}')
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (fetched_at, target_url, uuid)
SETTINGS index_granularity = 8192
COMMENT 'R website fetch raw local table. OLAP crawl analytics; website registry projection stays in Data_R_Package_Service.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.r_website_fetch_raw
ON CLUSTER statground_cluster
AS `Data_R_Package_Raw`.r_website_fetch_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Raw', 'r_website_fetch_raw_local', cityHash64(url_hash))
COMMENT 'Distributed R website fetch raw table.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.r_website_feed_item_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Feed item event UUID v7',
    uuid_website Nullable(UUID) COMMENT 'R website UUID when known',
    uuid_feed Nullable(UUID) COMMENT 'R website feed UUID when known',
    feed_url String COMMENT 'Feed URL',
    item_url String COMMENT 'Feed item URL',
    item_hash String COMMENT 'SHA-256 hex hash for feed item identity',
    title String COMMENT 'Feed item title',
    summary String COMMENT 'Feed item summary/snippet',
    author String COMMENT 'Feed item author',
    published_at Nullable(DateTime64(3, 'Asia/Seoul')) COMMENT 'Feed item published timestamp',
    collected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Collector timestamp',
    source LowCardinality(String) COMMENT 'Collector source name',
    payload_json String COMMENT 'Feed item payload JSON',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Raw/r_website_feed_item_raw_local', '{replica}', collected_at)
PARTITION BY toYYYYMM(collected_at)
ORDER BY (collected_at, feed_url, item_hash, uuid)
SETTINGS index_granularity = 8192
COMMENT 'R website feed item raw local table. OLAP content intelligence input.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.r_website_feed_item_raw
ON CLUSTER statground_cluster
AS `Data_R_Package_Raw`.r_website_feed_item_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Raw', 'r_website_feed_item_raw_local', cityHash64(item_hash))
COMMENT 'Distributed R website feed item raw table.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.r_website_package_mention_raw_local
ON CLUSTER statground_cluster
(
    uuid UUID COMMENT 'Mention event UUID v7',
    uuid_website Nullable(UUID) COMMENT 'R website UUID when known',
    source_url String COMMENT 'Page or feed item URL where mention was found',
    repository LowCardinality(String) COMMENT 'Package repository such as CRAN, Bioconductor, R-universe, or unknown',
    package_name String COMMENT 'Mentioned package name',
    mention_context String COMMENT 'Short text context for mention',
    confidence Float64 COMMENT 'Mention confidence 0-1',
    detected_at DateTime64(3, 'Asia/Seoul') COMMENT 'Mention detection timestamp',
    source LowCardinality(String) COMMENT 'Collector source name',
    payload_json String COMMENT 'Mention payload JSON'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Raw/r_website_package_mention_raw_local', '{replica}', detected_at)
PARTITION BY toYYYYMM(detected_at)
ORDER BY (detected_at, repository, package_name, source_url, uuid)
SETTINGS index_granularity = 8192
COMMENT 'R website package mention raw local table. OLAP web-presence signal input.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Raw`.r_website_package_mention_raw
ON CLUSTER statground_cluster
AS `Data_R_Package_Raw`.r_website_package_mention_raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Raw', 'r_website_package_mention_raw_local', cityHash64(package_name))
COMMENT 'Distributed R website package mention raw table.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.r_website_profile_daily_local
ON CLUSTER statground_cluster
(
    report_date Date COMMENT 'Report date',
    uuid_website UUID COMMENT 'R website UUID converted to ClickHouse UUID',
    canonical_url String COMMENT 'Canonical website URL',
    host String COMMENT 'Website host',
    primary_category_code LowCardinality(String) COMMENT 'Primary category from service registry snapshot',
    trust_tier UInt8 COMMENT 'Trust tier from service registry snapshot',
    status_code UInt8 COMMENT 'Website status from service registry snapshot',
    freshness_score Float64 COMMENT 'Freshness score 0-100',
    activity_score Float64 COMMENT 'Activity score 0-100',
    authority_score Float64 COMMENT 'Authority score 0-100',
    technical_quality_score Float64 COMMENT 'Technical quality score 0-100',
    r_relevance_score Float64 COMMENT 'R relevance score 0-100',
    overall_score Float64 COMMENT 'Overall website score 0-100',
    report_json String COMMENT 'Additional report JSON',
    computed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Mart computation timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Mart/r_website_profile_daily_local', '{replica}', computed_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, primary_category_code, uuid_website)
SETTINGS index_granularity = 8192
COMMENT 'Daily R website profile mart. Supports web_r_go R website directory/report pages.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.r_website_profile_daily
ON CLUSTER statground_cluster
AS `Data_R_Package_Mart`.r_website_profile_daily_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Mart', 'r_website_profile_daily_local', cityHash64(toString(uuid_website)))
COMMENT 'Distributed daily R website profile mart.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.package_web_presence_daily_local
ON CLUSTER statground_cluster
(
    report_date Date COMMENT 'Report date',
    repository LowCardinality(String) COMMENT 'Package repository',
    package_name String COMMENT 'Package name',
    mention_count UInt64 COMMENT 'Package mentions in R websites during the report window',
    unique_website_count UInt64 COMMENT 'Unique R websites mentioning the package',
    authority_weighted_mentions Float64 COMMENT 'Mentions weighted by website authority',
    trend_score Float64 COMMENT 'Relative web-presence trend score',
    report_json String COMMENT 'Additional report JSON',
    computed_at DateTime64(3, 'Asia/Seoul') COMMENT 'Mart computation timestamp'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_R_Package_Mart/package_web_presence_daily_local', '{replica}', computed_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, repository, package_name)
SETTINGS index_granularity = 8192
COMMENT 'Daily package web-presence mart. Supports package detail Web tab in web_r_go.';

CREATE TABLE IF NOT EXISTS `Data_R_Package_Mart`.package_web_presence_daily
ON CLUSTER statground_cluster
AS `Data_R_Package_Mart`.package_web_presence_daily_local
ENGINE = Distributed('statground_cluster', 'Data_R_Package_Mart', 'package_web_presence_daily_local', cityHash64(package_name))
COMMENT 'Distributed daily package web-presence mart.';
