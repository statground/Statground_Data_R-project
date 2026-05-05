CREATE TABLE IF NOT EXISTS Data_R_Blogger_Log.log
ON CLUSTER statground_cluster
AS Data_R_Blogger_Log.log_local
ENGINE = Distributed('statground_cluster', 'Data_R_Blogger_Log', 'log_local', cityHash64(toString(uuid)))
COMMENT 'R-bloggers crawl log distributed table; writes route by uuid';
