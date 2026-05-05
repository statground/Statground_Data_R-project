CREATE TABLE IF NOT EXISTS Data_R_Project_Mastodon_Raw.raw
ON CLUSTER statground_cluster
AS Data_R_Project_Mastodon_Raw.raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Project_Mastodon_Raw', 'raw_local', cityHash64(status_id))
COMMENT 'Distributed Mastodon raw status snapshot table across statground_cluster; writes route by status_id; OLAP raw cache only; SSOT 아님';
