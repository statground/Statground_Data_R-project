-- Compatibility migration for early Data_R_Package installs where the Kafka
-- queue was created in Data_R_Package_Raw before the Log namespace was added.
-- The durable full DDL in 001_data_r_package_core.sql already creates
-- Data_R_Package_Log directly.

DROP TABLE IF EXISTS `Data_R_Package_Raw`.mv_kafka_r_package_event_to_raw
ON CLUSTER statground_cluster;

DROP TABLE IF EXISTS `Data_R_Package_Raw`.kafka_r_package_event_queue
ON CLUSTER statground_cluster;

CREATE DATABASE IF NOT EXISTS `Data_R_Package_Log`
ON CLUSTER statground_cluster
COMMENT 'R package Kafka queues and ingestion logs. ClickHouse OLAP operational log layer.';

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
