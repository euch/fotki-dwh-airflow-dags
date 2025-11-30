insert into dm.images_collection
(abs_filename, rel_filename, preview, latest_caption, exif)
with latest_caption as (
	select
		distinct on (c.create_ts, c.hash)
		m.hash,
		c.caption
	from core.caption c
	join core.metadata m on m.hash = c.hash
	where c.delete is false
	order by c.create_ts desc
)
select
	trp.abs_filename,
	rel_filename,
	m.preview,
	lc.caption as latest_caption,
	m.exif
from
	core.tree_rel_path trp
join core.tree t on t.abs_filename = trp.abs_filename
join core.metadata m on	m.abs_filename = trp.abs_filename
join latest_caption lc on lc.hash = m.hash
where t."type" = 'collection'
on conflict (abs_filename) do nothing;
