insert
	into
	core.tree_rel_path (abs_filename,
	rel_filename)
select
	abs_filename,
	REGEXP_REPLACE(abs_filename, %(pattern_archive)s, '') as rel_filename
from
	core.tree
where
	"type" = 'archive'
union
select
	abs_filename,
	REGEXP_REPLACE(abs_filename, %(pattern_collection)s, '') as rel_filename
from
	core.tree
where
	"type" = 'collection'
union
select
	abs_filename,
	REGEXP_REPLACE(abs_filename, %(pattern_trash)s, '') as rel_filename
from
	core.tree
where
	"type" = 'trash'
on
	conflict (abs_filename) do nothing;