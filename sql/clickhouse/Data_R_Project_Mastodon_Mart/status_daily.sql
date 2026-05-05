CREATE TABLE IF NOT EXISTS `Data_R_Project_Mastodon_Mart`.status_daily
ON CLUSTER statground_cluster
AS `Data_R_Project_Mastodon_Mart`.status_daily_local
ENGINE = Distributed('statground_cluster', 'Data_R_Project_Mastodon_Mart', 'status_daily_local', cityHash64(instance_host, account_acct))
COMMENT 'Distributed daily R Project Mastodon status mart.';
