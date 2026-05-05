CREATE TABLE IF NOT EXISTS Data_R_Project_Mastodon_Service.board
ON CLUSTER statground_cluster
AS Data_R_Project_Mastodon_Service.board_local
ENGINE = Distributed('statground_cluster', 'Data_R_Project_Mastodon_Service', 'board_local', cityHash64(toString(uuid)))
COMMENT 'Mastodon curated Korean board/version rows distributed table; writes route by uuid';
