-- DROP SCHEMA raw;

CREATE SCHEMA raw AUTHORIZATION postgres;
-- raw.tree_archive definition

-- Drop table

-- DROP TABLE raw.tree_archive;

CREATE TABLE raw.tree_archive (
	abs_filename varchar NOT NULL,
	last_modified_ts int8 NOT NULL,
	"size" int8 NOT NULL
);


-- raw.tree_collection definition

-- Drop table

-- DROP TABLE raw.tree_collection;

CREATE TABLE raw.tree_collection (
	abs_filename varchar NOT NULL,
	last_modified_ts int8 NOT NULL,
	"size" int8 NOT NULL
);


-- raw.tree_trash definition

-- Drop table

-- DROP TABLE raw.tree_trash;

CREATE TABLE raw.tree_trash (
	abs_filename varchar NOT NULL,
	last_modified_ts int8 NOT NULL,
	"size" int8 NOT NULL
);


-- raw.ignored source

CREATE OR REPLACE VIEW raw.ignored
AS SELECT abs_filename,
    type,
    last_modified_ts,
    size
   FROM ( SELECT tree_collection.abs_filename,
            'collection'::text AS type,
            tree_collection.last_modified_ts,
            tree_collection.size
           FROM raw.tree_collection
        UNION
         SELECT tree_trash.abs_filename,
            'trash'::text AS type,
            tree_trash.last_modified_ts,
            tree_trash.size
           FROM raw.tree_trash
        UNION
         SELECT tree_archive.abs_filename,
            'archive'::text AS type,
            tree_archive.last_modified_ts,
            tree_archive.size
           FROM raw.tree_archive) e
  WHERE NOT (EXISTS ( SELECT 1
           FROM raw.tree_all t
          WHERE t.abs_filename::text = e.abs_filename::text));


-- raw.tree_all source

CREATE OR REPLACE VIEW raw.tree_all
AS SELECT abs_filename,
    type,
    last_modified_ts,
    size
   FROM ( SELECT tree_collection.abs_filename,
            'collection'::text AS type,
            tree_collection.last_modified_ts,
            tree_collection.size
           FROM raw.tree_collection
        UNION
         SELECT tree_trash.abs_filename,
            'trash'::text AS type,
            tree_trash.last_modified_ts,
            tree_trash.size
           FROM raw.tree_trash
        UNION
         SELECT tree_archive.abs_filename,
            'archive'::text AS type,
            tree_archive.last_modified_ts,
            tree_archive.size
           FROM raw.tree_archive) unnamed_subquery
  WHERE abs_filename::text !~~* '%/CaptureOne/%'::text AND abs_filename::text !~~* '%.xmp'::text AND size <> 0 AND abs_filename::text !~~ '%/.%'::text AND abs_filename::text !~~ '%/tree.csv'::text AND (abs_filename::text ~~* '%.RW2'::text OR abs_filename::text ~~* '%.JPG'::text OR abs_filename::text ~~* '%.JPEG'::text OR abs_filename::text ~~* '%.HEIC'::text OR abs_filename::text ~~* '%.NEF'::text OR abs_filename::text ~~* '%.GIF'::text OR abs_filename::text ~~* '%.PNG'::text);