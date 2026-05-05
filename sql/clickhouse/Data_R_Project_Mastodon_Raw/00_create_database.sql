CREATE DATABASE IF NOT EXISTS `Data_R_Project_Mastodon_Raw`
ON CLUSTER statground_cluster
COMMENT 'R Project Mastodon raw source snapshots. OLAP raw storage; service/mart DBs serve Web-R.';
