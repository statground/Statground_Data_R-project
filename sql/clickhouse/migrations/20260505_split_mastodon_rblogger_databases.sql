-- Split legacy Web-R R-project source databases into Raw / Log / Service / Mart namespaces.
-- The live SQL copy is mirrored in Statground_SQL/Clickhouse/Data_R_Project_Migration.
--
-- Apply the split DDL directories first, then copy the old rows from:
--   webr_mastodon -> Data_R_Project_Mastodon_*
--   webr_rblogger -> Data_R_Blogger_*
--
-- Keep legacy databases until live counts and Web-R board reads have been
-- verified. They are rollback sources and should not be dropped as part of this
-- migration.

INSERT INTO Data_R_Project_Mastodon_Raw.raw
SELECT *
FROM webr_mastodon.raw;

INSERT INTO Data_R_Project_Mastodon_Log.log
SELECT *
FROM webr_mastodon.log;

INSERT INTO Data_R_Project_Mastodon_Log.kafka_ingest_error
SELECT *
FROM webr_mastodon.kafka_ingest_error;

INSERT INTO Data_R_Project_Mastodon_Service.board
SELECT *
FROM webr_mastodon.board;

INSERT INTO Data_R_Blogger_Raw.raw
SELECT *
FROM webr_rblogger.raw;

INSERT INTO Data_R_Blogger_Log.log
SELECT *
FROM webr_rblogger.log;

INSERT INTO Data_R_Blogger_Service.board
SELECT *
FROM webr_rblogger.board;
