CREATE TABLE IF NOT EXISTS Data_R_Project_Mastodon_Log.kafka_ingest_error
ON CLUSTER statground_cluster
AS Data_R_Project_Mastodon_Log.kafka_ingest_error_local
ENGINE = Distributed('statground_cluster', 'Data_R_Project_Mastodon_Log', 'kafka_ingest_error_local', rand())
COMMENT 'Distributed Mastodon Kafka ingestion error table across statground_cluster; OLAP 로그 전용; SSOT 아님';
