CREATE TABLE IF NOT EXISTS Data_R_Project_Mastodon_Log.log
ON CLUSTER statground_cluster
AS Data_R_Project_Mastodon_Log.log_local
ENGINE = Distributed('statground_cluster', 'Data_R_Project_Mastodon_Log', 'log_local', cityHash64(toString(uuid)))
COMMENT 'Mastodon crawler pipeline log distributed table; writes route by uuid';
