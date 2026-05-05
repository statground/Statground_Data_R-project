CREATE OR REPLACE TABLE Data_R_Project_Mastodon_Log.kafka_webr_events_queue
ON CLUSTER statground_cluster
(
    event_uuid String COMMENT 'UUID v7 Kafka event identifier as 36-char string from GitHub producer; parsed to UUID in target raw table',
    source LowCardinality(String) COMMENT 'Producer source such as github_actions',
    host LowCardinality(String) COMMENT 'Producer host or GitHub runner name',
    uuid_user String COMMENT 'Optional user UUID v7 string; empty for scheduled crawler events',
    ip String COMMENT 'Producer/client IP string; ClickHouse MVs normalize to IPv6 with :: default',
    url String COMMENT 'Source URL related to the event; normally Mastodon status URL',
    event_type LowCardinality(String) COMMENT 'WebR event type; expected webr.mastodon.raw.v1 for Mastodon raw status snapshots',
    payload String COMMENT 'Event-specific payload JSON string; parsed by Materialized Views into raw table',
    created_at String COMMENT 'Producer event creation timestamp string in Asia/Seoul format yyyy-MM-dd HH:mm:ss.SSS'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-platform:19092',
    kafka_topic_list = 'webr.events',
    kafka_group_name = 'clickhouse_data_r_project_mastodon_events_v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream'
COMMENT 'WebR Mastodon Kafka Engine stream table; consumes webr.events and routes webr.mastodon.* events through Materialized Views; OLAP ingestion buffer only; SSOT 아님';
