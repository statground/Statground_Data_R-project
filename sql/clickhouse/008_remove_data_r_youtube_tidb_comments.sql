SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

ALTER DATABASE `Data_R_Youtube_Raw`
ON CLUSTER statground_cluster
MODIFY COMMENT 'R YouTube raw collection events, video snapshots, transcript segments, comments, and package mentions. ClickHouse owns collector operational state through Data_R_Youtube_Log and Data_R_Youtube_Service.';
