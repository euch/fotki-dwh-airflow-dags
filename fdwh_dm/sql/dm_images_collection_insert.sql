insert into dm.images_collection
(abs_filename, rel_filename, preview, latest_caption, exif)
select
	trp.abs_filename,
	rel_filename,
	m.preview,
	lc.caption as latest_caption,
	m.exif
from
	core.tree_rel_path trp
join core.tree t on t.abs_filename = trp.abs_filename
left join core.metadata m on m.abs_filename = trp.abs_filename
left join core.latest_caption lc on lc.hash = m.hash and lc."delete" is false
where t."type" = 'collection'
on conflict (abs_filename) do nothing;
