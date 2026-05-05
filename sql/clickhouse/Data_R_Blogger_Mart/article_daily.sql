CREATE TABLE IF NOT EXISTS `Data_R_Blogger_Mart`.article_daily
ON CLUSTER statground_cluster
AS `Data_R_Blogger_Mart`.article_daily_local
ENGINE = Distributed('statground_cluster', 'Data_R_Blogger_Mart', 'article_daily_local', rand())
COMMENT 'Distributed daily R-bloggers article mart.';
