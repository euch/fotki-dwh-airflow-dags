insert
	into core.metadata
    (abs_filename, hash, exif, preview)
values (%s,%s,%s,%s)
on conflict (abs_filename) do
update
set
	hash = EXCLUDED.hash,
	exif = EXCLUDED.exif,
	preview = EXCLUDED.preview
where
	core.metadata.hash is distinct from	EXCLUDED.hash
	or core.metadata.exif::text is distinct from EXCLUDED.exif::text
	or core.metadata.preview is distinct from EXCLUDED.preview