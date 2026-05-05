CREATE DATABASE IF NOT EXISTS `Data_R_Youtube_Log`
ON CLUSTER statground_cluster
COMMENT 'R YouTube Kafka queues, ingestion errors, and collection logs. ClickHouse operational log layer.';
