-- DROP SCHEMA duplicates;

CREATE SCHEMA duplicates;
-- duplicates.collection_duplicates definition

-- Drop table

-- DROP TABLE collection_duplicates;

CREATE TABLE collection_duplicates (
	abs_filename varchar NOT NULL,
	hash varchar NOT NULL,
	preview bytea NULL,
	cnt int8 NOT NULL,
	"delete" bool DEFAULT false NOT NULL,
	CONSTRAINT collection_duplicates_pk PRIMARY KEY (abs_filename)
);


-- duplicates.collection_repeated_imports source

CREATE OR REPLACE VIEW collection_repeated_imports
AS WITH col_ts_dirs AS (
         SELECT (string_to_array(collection_duplicates.abs_filename::text, '/'::text, NULL::text))[5] AS col_ts_dir,
            collection_duplicates.abs_filename,
            collection_duplicates.hash
           FROM duplicates.collection_duplicates
          WHERE collection_duplicates.abs_filename::text ~~ '/storage/fotki/collection/%'::text
        ), col_ts_dir_dates AS (
         SELECT (regexp_matches(col_ts_dirs.col_ts_dir, '(\d{4})-(\d{2})-(\d{2})'::text))[1] AS year,
            (regexp_matches(col_ts_dirs.col_ts_dir, '(\d{4})-(\d{2})-(\d{2})'::text))[2] AS month,
            (regexp_matches(col_ts_dirs.col_ts_dir, '(\d{4})-(\d{2})-(\d{2})'::text))[3] AS day,
            col_ts_dirs.hash,
            col_ts_dirs.col_ts_dir,
            col_ts_dirs.abs_filename
           FROM col_ts_dirs
        )
 SELECT abs_filename
   FROM col_ts_dir_dates a
  WHERE col_ts_dir ~~ '%-auto'::text AND (EXISTS ( SELECT 1
           FROM col_ts_dir_dates m
          WHERE m.col_ts_dir ~~ (((((a.year || '-'::text) || a.month) || '-'::text) || a.day) || '%'::text) AND m.col_ts_dir !~~ '%-auto'::text));