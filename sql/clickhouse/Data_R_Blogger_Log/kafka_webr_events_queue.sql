CREATE OR REPLACE TABLE Data_R_Blogger_Log.kafka_webr_events_queue
ON CLUSTER statground_cluster
(
    event_uuid String COMMENT 'Kafka message UUID string',
    source String COMMENT 'Producer source string',
    host String COMMENT 'Producer host string',
    uuid_user Nullable(String) COMMENT 'Optional user UUID string',
    ip String COMMENT 'Producer IP string',
    url String COMMENT 'Source URL string',
    event_type String COMMENT 'R-blogger event type discriminator',
    payload String COMMENT 'R-blogger row payload JSON string',
    created_at DateTime64(3, 'Asia/Seoul') COMMENT 'Producer event timestamp'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-platform:19092',
    kafka_topic_list = 'webr.events',
    kafka_group_name = 'clickhouse_data_r_blogger_events_v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream'
COMMENT 'Web-R R-bloggers Kafka Engine stream table; consumes webr.events and routes R-blogger events through Materialized Views; OLAP ingestion buffer only';
