with counts as (
	select
		(select count(*) from core.tree where "type"='archive') as known_files_archive,
		(select count(*) from core.tree where "type"='collection') as known_files_collection,
		(select count(*) from core.tree where "type"='trash') as known_files_trash,
		(select count(*) from core.metadata) AS metadata_total,
		(select count(*) from core.metadata where preview is null or length(preview) < 100) as metadata_nopreview,
		(select count(*) from core.metadata where exif is null or exif::text = '{}') as metadata_noexif,
		(select count(*) from core.metadata where exif is not null and exif::text <> '{}' and length(preview) > 100) as metadata_full

)
insert into dm.counts
select
	now(),
	known_files_archive,
	known_files_collection,
	known_files_trash,
	known_files_archive + known_files_collection + known_files_trash as known_files_total,
	metadata_total::float / (known_files_archive + known_files_collection) as metadata_ratio,
	metadata_total,
	metadata_nopreview,
	metadata_noexif,
	metadata_full
from counts;
