-- DROP SCHEMA core;

CREATE SCHEMA core AUTHORIZATION postgres;

-- DROP SEQUENCE core.caption_conf_id_seq;

CREATE SEQUENCE core.caption_conf_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;-- core.caption definition

-- Drop table

-- DROP TABLE core.caption;

CREATE TABLE core.caption ( hash varchar NOT NULL, caption_conf_id int4 NOT NULL, caption text NOT NULL, create_ts timestamptz DEFAULT now() NOT NULL);
CREATE INDEX caption_caption_idx ON core.caption USING btree (caption);


-- core.caption_conf definition

-- Drop table

-- DROP TABLE core.caption_conf;

CREATE TABLE core.caption_conf ( id serial4 NOT NULL, model varchar NOT NULL, prompt varchar NOT NULL, CONSTRAINT caption_conf_pk PRIMARY KEY (id));


-- core.tree definition

-- Drop table

-- DROP TABLE core.tree;

CREATE TABLE core.tree ( abs_filename varchar NOT NULL, "size" int8 NOT NULL, last_modified_ts int8 NOT NULL, "type" varchar NOT NULL, base_path varchar NOT NULL, relative_path varchar NOT NULL, CONSTRAINT tree_pk PRIMARY KEY (abs_filename));


-- core.caption_conf_selection definition

-- Drop table

-- DROP TABLE core.caption_conf_selection;

CREATE TABLE core.caption_conf_selection ( selection_ts timestamptz DEFAULT now() NOT NULL, caption_conf_id int4 NOT NULL, CONSTRAINT caption_conf_selected_unique UNIQUE (selection_ts), CONSTRAINT caption_conf_selection_caption_conf_fk FOREIGN KEY (caption_conf_id) REFERENCES core.caption_conf(id), CONSTRAINT caption_conf_selection_caption_conf_fk_1 FOREIGN KEY (caption_conf_id) REFERENCES core.caption_conf(id));


-- core.metadata definition

-- Drop table

-- DROP TABLE core.metadata;

CREATE TABLE core.metadata ( abs_filename varchar NOT NULL, hash varchar NOT NULL, exif json NULL, preview bytea NULL, CONSTRAINT metadata_pk PRIMARY KEY (abs_filename), CONSTRAINT metadata_fk FOREIGN KEY (abs_filename) REFERENCES core.tree(abs_filename) ON DELETE CASCADE ON UPDATE RESTRICT);
CREATE INDEX metadata_hash_idx ON core.metadata USING btree (hash);


-- core.current_caption_conf source

CREATE OR REPLACE VIEW core.current_caption_conf
AS SELECT cc.id,
    cc.model,
    cc.prompt
   FROM core.caption_conf cc
     JOIN core.current_caption_conf_selection cccs ON cccs.caption_conf_id = cc.id;


-- core.current_caption_conf_selection source

CREATE OR REPLACE VIEW core.current_caption_conf_selection
AS SELECT DISTINCT ON (selection_ts) caption_conf_id
   FROM core.caption_conf_selection ccs
  ORDER BY selection_ts DESC
 LIMIT 1;


-- core.latest_caption source

CREATE OR REPLACE VIEW core.latest_caption
AS SELECT DISTINCT ON (create_ts, hash) hash,
    caption
   FROM core.caption c
  ORDER BY create_ts DESC;

-- DROP SCHEMA dm;

CREATE SCHEMA dm AUTHORIZATION postgres;
-- dm.caption_count_by_type definition

-- Drop table

-- DROP TABLE dm.caption_count_by_type;

CREATE TABLE dm.caption_count_by_type ( ts timestamptz DEFAULT now() NOT NULL, "type" varchar NOT NULL, caption_conf_id int4 NULL, model varchar NOT NULL, count int8 NOT NULL);


-- dm.collection_duplicates definition

-- Drop table

-- DROP TABLE dm.collection_duplicates;

CREATE TABLE dm.collection_duplicates ( abs_filename varchar NOT NULL, hash varchar NOT NULL, preview bytea NULL, cnt int8 NOT NULL, "delete" bool DEFAULT false NOT NULL, CONSTRAINT collection_duplicates_pk PRIMARY KEY (abs_filename));


-- dm.counts definition

-- Drop table

-- DROP TABLE dm.counts;

CREATE TABLE dm.counts ( now timestamptz NULL, known_files_archive int8 NULL, known_files_collection int8 NULL, known_files_trash int8 NULL, known_files_total int8 NULL, metadata_ratio float8 NULL, metadata_total int8 NULL, metadata_nopreview int8 NULL, metadata_noexif int8 NULL, metadata_full int8 NULL);


-- dm.files_and_types definition

-- Drop table

-- DROP TABLE dm.files_and_types;

CREATE TABLE dm.files_and_types ( ts timestamptz NULL, file_type text NULL, cnt int8 NULL);


-- dm.preview_count_by_type definition

-- Drop table

-- DROP TABLE dm.preview_count_by_type;

CREATE TABLE dm.preview_count_by_type ( ts timestamptz DEFAULT now() NOT NULL, "type" varchar NOT NULL, count int8 NOT NULL);


-- dm.total_count_by_type definition

-- Drop table

-- DROP TABLE dm.total_count_by_type;

CREATE TABLE dm.total_count_by_type ( ts timestamptz DEFAULT now() NOT NULL, "type" varchar NOT NULL, count int8 NOT NULL);


-- dm.all_body_counts source

CREATE OR REPLACE VIEW dm.all_body_counts
AS SELECT exif ->> 'EXIF BodySerialNumber'::text AS body_sn,
    exif ->> 'Image Make'::text AS maker,
    exif ->> 'Image Model'::text AS model,
    count(*) AS col_img_count,
    min((exif ->> 'MakerNote TotalShutterReleases'::text)::integer) AS col_min_shutter_count,
    max((exif ->> 'MakerNote TotalShutterReleases'::text)::integer) AS col_max_shutter_count
   FROM core.metadata m
  GROUP BY (exif ->> 'EXIF BodySerialNumber'::text), (exif ->> 'Image Make'::text), (exif ->> 'Image Model'::text);


-- dm.col_body_counts source

CREATE OR REPLACE VIEW dm.col_body_counts
AS SELECT m.exif ->> 'EXIF BodySerialNumber'::text AS body_sn,
    m.exif ->> 'Image Make'::text AS maker,
    m.exif ->> 'Image Model'::text AS model,
    count(*) AS col_img_count,
    min((m.exif ->> 'MakerNote TotalShutterReleases'::text)::integer) AS col_min_shutter_count,
    max((m.exif ->> 'MakerNote TotalShutterReleases'::text)::integer) AS col_max_shutter_count
   FROM core.metadata m
     JOIN core.tree t ON t.abs_filename::text = m.abs_filename::text AND t.type::text = 'collection'::text
  GROUP BY (m.exif ->> 'EXIF BodySerialNumber'::text), (m.exif ->> 'Image Make'::text), (m.exif ->> 'Image Model'::text);


-- dm.col_dirs_by_count source

CREATE OR REPLACE VIEW dm.col_dirs_by_count
AS SELECT (string_to_array(abs_filename::text, '/'::text, NULL::text))[5] AS col_ts_dir,
    array_agg(abs_filename) AS array_agg,
    count(*) AS count
   FROM core.tree t
  WHERE type::text = 'collection'::text
  GROUP BY ((string_to_array(abs_filename::text, '/'::text, NULL::text))[5])
  ORDER BY (count(*)) DESC, ((string_to_array(abs_filename::text, '/'::text, NULL::text))[5]);


-- dm.col_images source

CREATE OR REPLACE VIEW dm.col_images
AS SELECT t.abs_filename,
    t.relative_path AS rel_filename,
    split_part(t.abs_filename::text, '/'::text, '-1'::integer) AS short_filename,
    "left"(t.abs_filename::text, length(t.abs_filename::text) - POSITION(('/'::text) IN (reverse(t.abs_filename::text)))) AS directory,
    m.preview,
    lc.caption AS latest_caption,
    m.exif,
    m.exif ->> 'EXIF BodySerialNumber'::text AS exif_body_sn,
    m.exif ->> 'Image Make'::text AS exif_body_maker,
    m.exif ->> 'Image Model'::text AS exif_body_model
   FROM core.tree t
     LEFT JOIN core.metadata m ON m.abs_filename::text = t.abs_filename::text
     LEFT JOIN core.latest_caption lc ON lc.hash::text = m.hash::text
  WHERE t.type::text = 'collection'::text AND (upper(split_part(t.relative_path::text, '.'::text, '-1'::integer)) = ANY (ARRAY['JPG'::text, 'JPEG'::text, 'PNG'::text, 'GIF'::text, 'HEIC'::text, 'NEF'::text, 'RW2'::text]));


-- dm.col_images_birds source

CREATE OR REPLACE VIEW dm.col_images_birds
AS SELECT abs_filename,
    rel_filename,
    short_filename,
    directory,
    preview,
    latest_caption,
    exif
   FROM dm.col_images
  WHERE latest_caption ~~* '%bird%'::text OR rel_filename::text ~~* '%птиц%'::text;


-- dm.col_missing_caption source

CREATE OR REPLACE VIEW dm.col_missing_caption
AS SELECT DISTINCT ON (m.hash) m.hash,
    m.preview,
    m.abs_filename
   FROM core.metadata m
     LEFT JOIN core.caption c ON c.hash::text = m.hash::text
     JOIN core.tree t ON t.abs_filename::text = m.abs_filename::text
  WHERE c.hash IS NULL AND m.preview IS NOT NULL AND t.type::text = 'collection'::text
  ORDER BY m.hash, m.abs_filename DESC;


-- dm.col_nopreview source

CREATE OR REPLACE VIEW dm.col_nopreview
AS SELECT m.abs_filename,
    regexp_replace(m.abs_filename::text, '^.*[.]([^.]+)$'::text, '\1'::text) AS file_type,
    m.exif
   FROM core.metadata m
     JOIN core.tree t ON t.abs_filename::text = m.abs_filename::text AND t.type::text = 'collection'::text
  WHERE m.preview IS NULL;


-- dm.col_videos source

CREATE OR REPLACE VIEW dm.col_videos
AS SELECT abs_filename,
    relative_path AS rel_filename,
    split_part(abs_filename::text, '/'::text, '-1'::integer) AS short_filename,
    "left"(abs_filename::text, length(abs_filename::text) - POSITION(('/'::text) IN (reverse(abs_filename::text)))) AS directory
   FROM core.tree t
  WHERE type::text = 'collection'::text AND (upper(split_part(relative_path::text, '.'::text, '-1'::integer)) = ANY (ARRAY['MOV'::text, 'MP4'::text]));


-- dm.weird_files source

CREATE OR REPLACE VIEW dm.weird_files
AS WITH weird_file_types AS (
         SELECT DISTINCT ON (fat.file_type) fat.file_type,
            fat.cnt,
            fat.ts
           FROM dm.files_and_types fat
          WHERE lower(fat.file_type) <> ALL (ARRAY['nef'::text, 'jpg'::text, 'jpeg'::text, 'png'::text, 'rw2'::text, 'mov'::text, 'heic'::text, 'avi'::text, 'mp4'::text, 'bmp'::text, 'gif'::text])
          ORDER BY fat.file_type, fat.ts DESC
        )
 SELECT file_type,
    cnt,
    ts AS last_seen,
    ARRAY( SELECT t.abs_filename
           FROM core.tree t
          WHERE t.abs_filename::text ~~ ('%.'::text || wtf.file_type) OR wtf.file_type = t.abs_filename::text) AS abs_filenames
   FROM weird_file_types wtf;

-- DROP SCHEMA log;

CREATE SCHEMA log AUTHORIZATION postgres;
-- log.core_deleted_log definition

-- Drop table

-- DROP TABLE log.core_deleted_log;

CREATE TABLE log.core_deleted_log ( abs_filename varchar NOT NULL, tree_del_ts timestamptz NOT NULL, tree_add_ts timestamptz NOT NULL, metadata_add_ts timestamptz NULL, caption_add_ts timestamptz NULL, hash varchar NULL, CONSTRAINT edm_deleted_log_pk PRIMARY KEY (abs_filename, tree_del_ts));


-- log.core_log definition

-- Drop table

-- DROP TABLE log.core_log;

CREATE TABLE log.core_log ( abs_filename varchar NOT NULL, tree_add_ts timestamptz NOT NULL, metadata_add_ts timestamptz NULL, caption_add_ts timestamptz NULL, hash varchar NULL, CONSTRAINT edm_log_pk PRIMARY KEY (abs_filename));

-- DROP SCHEMA raw;

CREATE SCHEMA raw AUTHORIZATION postgres;
-- raw.tree_archive definition

-- Drop table

-- DROP TABLE raw.tree_archive;

CREATE TABLE raw.tree_archive ( abs_filename varchar NOT NULL, last_modified_ts int8 NOT NULL, "size" int8 NOT NULL, snapshot_time timestamptz NOT NULL, base_path varchar NOT NULL, relative_path varchar NOT NULL);


-- raw.tree_collection definition

-- Drop table

-- DROP TABLE raw.tree_collection;

CREATE TABLE raw.tree_collection ( abs_filename varchar NOT NULL, last_modified_ts int8 NOT NULL, "size" int8 NOT NULL, snapshot_time timestamptz NOT NULL, base_path varchar NOT NULL, relative_path varchar NOT NULL);


-- raw.tree_trash definition

-- Drop table

-- DROP TABLE raw.tree_trash;

CREATE TABLE raw.tree_trash ( abs_filename varchar NOT NULL, last_modified_ts int8 NOT NULL, "size" int8 NOT NULL, snapshot_time timestamptz NOT NULL, base_path varchar NOT NULL, relative_path varchar NOT NULL);


-- raw.tree_all source

CREATE OR REPLACE VIEW raw.tree_all
AS SELECT type,
    abs_filename,
    last_modified_ts,
    size,
    snapshot_time,
    base_path,
    relative_path
   FROM ( SELECT 'collection'::text AS type,
            tree_collection.abs_filename,
            tree_collection.last_modified_ts,
            tree_collection.size,
            tree_collection.snapshot_time,
            tree_collection.base_path,
            tree_collection.relative_path
           FROM raw.tree_collection
        UNION
         SELECT 'trash'::text AS type,
            tree_trash.abs_filename,
            tree_trash.last_modified_ts,
            tree_trash.size,
            tree_trash.snapshot_time,
            tree_trash.base_path,
            tree_trash.relative_path
           FROM raw.tree_trash
        UNION
         SELECT 'archive'::text AS type,
            tree_archive.abs_filename,
            tree_archive.last_modified_ts,
            tree_archive.size,
            tree_archive.snapshot_time,
            tree_archive.base_path,
            tree_archive.relative_path
           FROM raw.tree_archive) unnamed_subquery
  WHERE abs_filename::text !~~* '%/CaptureOne/%'::text AND abs_filename::text !~~* '%.xmp'::text AND size <> 0 AND abs_filename::text !~~ '%/.%'::text AND abs_filename::text !~~ '%/tree.csv'::text AND (abs_filename::text ~~* '%.RW2'::text OR abs_filename::text ~~* '%.JPG'::text OR abs_filename::text ~~* '%.JPEG'::text OR abs_filename::text ~~* '%.HEIC'::text OR abs_filename::text ~~* '%.NEF'::text OR abs_filename::text ~~* '%.GIF'::text OR abs_filename::text ~~* '%.PNG'::text OR abs_filename::text ~~* '%.MP4'::text OR abs_filename::text ~~* '%.MOV'::text);