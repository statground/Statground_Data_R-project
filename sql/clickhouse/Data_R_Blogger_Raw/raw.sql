CREATE TABLE IF NOT EXISTS Data_R_Blogger_Raw.raw
ON CLUSTER statground_cluster
AS Data_R_Blogger_Raw.raw_local
ENGINE = Distributed('statground_cluster', 'Data_R_Blogger_Raw', 'raw_local', cityHash64(url_hash))
COMMENT 'R-bloggers raw crawled article rows distributed table; writes route by url_hash';
