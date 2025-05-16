-- DROP SCHEMA dm;

CREATE SCHEMA dm AUTHORIZATION postgres;


-- dm.counts definition

-- Drop table

-- DROP TABLE dm.counts;

CREATE TABLE dm.counts (
	now timestamptz NULL,
	known_files_archive int8 NULL,
	known_files_collection int8 NULL,
	known_files_trash int8 NULL,
	known_files_total int8 NULL,
	metadata_ratio float8 NULL,
	metadata_total int8 NULL,
	metadata_nopreview int8 NULL,
	metadata_noexif int8 NULL,
	metadata_full int8 NULL
);


-- dm.files_and_types definition

-- Drop table

-- DROP TABLE dm.files_and_types;

CREATE TABLE dm.files_and_types (
	ts timestamptz NULL,
	file_type text NULL,
	cnt int8 NULL
);


-- dm.all_body_counts source

CREATE OR REPLACE VIEW dm.all_body_counts
AS SELECT exif ->> 'EXIF BodySerialNumber'::text AS body_sn,
    exif ->> 'Image Make'::text AS maker,
    exif ->> 'Image Model'::text AS model,
    count(*) AS col_img_count,
    min((exif ->> 'MakerNote TotalShutterReleases'::text)::integer) AS col_min_shutter_count,
    max((exif ->> 'MakerNote TotalShutterReleases'::text)::integer) AS col_max_shutter_count
   FROM edm.metadata m
  GROUP BY (exif ->> 'EXIF BodySerialNumber'::text), (exif ->> 'Image Make'::text), (exif ->> 'Image Model'::text);


-- dm.col_body_counts source

CREATE OR REPLACE VIEW dm.col_body_counts
AS SELECT m.exif ->> 'EXIF BodySerialNumber'::text AS body_sn,
    m.exif ->> 'Image Make'::text AS maker,
    m.exif ->> 'Image Model'::text AS model,
    count(*) AS col_img_count,
    min((m.exif ->> 'MakerNote TotalShutterReleases'::text)::integer) AS col_min_shutter_count,
    max((m.exif ->> 'MakerNote TotalShutterReleases'::text)::integer) AS col_max_shutter_count
   FROM edm.metadata m
     JOIN edm.tree t ON t.abs_filename::text = m.abs_filename::text AND t.type::text = 'collection'::text
  GROUP BY (m.exif ->> 'EXIF BodySerialNumber'::text), (m.exif ->> 'Image Make'::text), (m.exif ->> 'Image Model'::text);


-- dm.col_dirs_by_count source

CREATE OR REPLACE VIEW dm.col_dirs_by_count
AS SELECT (string_to_array(abs_filename::text, '/'::text, NULL::text))[5] AS col_ts_dir,
    array_agg(abs_filename) AS array_agg,
    count(*) AS count
   FROM edm.tree t
  WHERE type::text = 'collection'::text
  GROUP BY ((string_to_array(abs_filename::text, '/'::text, NULL::text))[5])
  ORDER BY (count(*)) DESC, ((string_to_array(abs_filename::text, '/'::text, NULL::text))[5]);


-- dm.col_images source

CREATE OR REPLACE VIEW dm.col_images
AS SELECT t.abs_filename,
    regexp_replace(t.abs_filename::text, '^.*/(.*)\..*'::text, '\1'::text) AS short_filename,
    m.preview,
    ad.caption_vit_gpt2 AS caption
   FROM edm.tree t
     LEFT JOIN edm.metadata m ON m.abs_filename::text = t.abs_filename::text
     LEFT JOIN edm.ai_description ad ON ad.abs_filename::text = t.abs_filename::text
  WHERE t.type::text = 'collection'::text
  ORDER BY t.abs_filename DESC;


-- dm.col_images_birds source

CREATE OR REPLACE VIEW dm.col_images_birds
AS SELECT abs_filename,
    short_filename,
    preview,
    caption
   FROM dm.col_images c
  WHERE caption::text ~~* '%bird%'::text;


-- dm.col_noexif source

CREATE OR REPLACE VIEW dm.col_noexif
AS SELECT m.abs_filename,
    regexp_replace(m.abs_filename::text, '^.*[.]([^.]+)$'::text, '\1'::text) AS file_type,
    m.preview
   FROM edm.metadata m
     JOIN edm.tree t ON t.abs_filename::text = m.abs_filename::text AND t.type::text = 'collection'::text
  WHERE m.exif IS NULL;


-- dm.col_nopreview source

CREATE OR REPLACE VIEW dm.col_nopreview
AS SELECT m.abs_filename,
    regexp_replace(m.abs_filename::text, '^.*[.]([^.]+)$'::text, '\1'::text) AS file_type,
    m.exif
   FROM edm.metadata m
     JOIN edm.tree t ON t.abs_filename::text = m.abs_filename::text AND t.type::text = 'collection'::text
  WHERE m.preview IS NULL;


-- dm.collection_duplicates_repeated_imports source

CREATE OR REPLACE VIEW dm.collection_duplicates_repeated_imports
AS WITH col_ts_dirs AS (
         SELECT (string_to_array(collection_duplicates.abs_filename::text, '/'::text, NULL::text))[5] AS col_ts_dir,
            collection_duplicates.abs_filename,
            collection_duplicates.hash
           FROM dm.collection_duplicates
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


-- dm.ignored_files source

CREATE OR REPLACE VIEW dm.ignored_files
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
           FROM edm.tree t
          WHERE t.abs_filename::text ~~ ('%.'::text || wtf.file_type) OR wtf.file_type = t.abs_filename::text) AS abs_filenames
   FROM weird_file_types wtf;