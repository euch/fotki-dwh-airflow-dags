-- DROP SCHEMA log;

CREATE SCHEMA log AUTHORIZATION postgres;
-- log.edm_deleted_log definition

-- Drop table

-- DROP TABLE log.edm_deleted_log;

CREATE TABLE log.edm_deleted_log (
	abs_filename varchar NOT NULL,
	tree_del_ts timestamptz NOT NULL,
	tree_add_ts timestamptz NOT NULL,
	metadata_add_ts timestamptz NULL,
	ai_description_add_ts timestamptz NULL,
	hash varchar NULL,
	CONSTRAINT edm_deleted_log_pk PRIMARY KEY (abs_filename, tree_del_ts)
);


-- log.edm_log definition

-- Drop table

-- DROP TABLE log.edm_log;

CREATE TABLE log.edm_log (
	abs_filename varchar NOT NULL,
	tree_add_ts timestamptz NOT NULL,
	metadata_add_ts timestamptz NULL,
	ai_description_add_ts timestamptz NULL,
	hash varchar NULL,
	CONSTRAINT edm_log_pk PRIMARY KEY (abs_filename)
);