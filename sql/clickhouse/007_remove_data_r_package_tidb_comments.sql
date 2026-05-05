SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

ALTER DATABASE `Data_R_Package_Raw`
ON CLUSTER statground_cluster
MODIFY COMMENT 'R package raw collection events. ClickHouse owns collector operational data through Data_R_Package_Log and Data_R_Package_Service.';

ALTER DATABASE `Data_R_Package_Service`
ON CLUSTER statground_cluster
MODIFY COMMENT 'R package normalized service tables. ClickHouse serving layer backed by Data_R_Package_Log and Data_R_Package_Raw.';

ALTER TABLE `Data_R_Package_Raw`.cran_package_snapshot_raw_local
ON CLUSTER statground_cluster
MODIFY COMMENT 'CRAN DESCRIPTION package snapshot raw local table. Package identity is projected into Data_R_Package_Service.';

ALTER TABLE `Data_R_Package_Raw`.r_website_fetch_raw_local
ON CLUSTER statground_cluster
MODIFY COMMENT 'R website fetch raw local table. OLAP crawl analytics; website registry projection stays in Data_R_Package_Service.';

ALTER TABLE `Data_R_Package_Service`.package_current_local
ON CLUSTER statground_cluster
MODIFY COMMENT 'Latest R package profile service table. Denormalized for web_r_go/API reads; Data_R_Package_Service is the serving identity projection.';

ALTER TABLE `Data_R_Package_Mart`.package_alert_event_local
ON CLUSTER statground_cluster
MODIFY COMMENT 'R package alert event mart. ClickHouse stores detected events and downstream workflow projections.';
