CREATE DATABASE IF NOT EXISTS `Data_R_Youtube_Raw`
ON CLUSTER statground_cluster
COMMENT 'R YouTube raw collection events, video snapshots, transcript segments, comments, and package mentions. ClickHouse owns collector operational state through Data_R_Youtube_Log and Data_R_Youtube_Service.';
