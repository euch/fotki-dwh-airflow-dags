insert into	dm.caption_count_by_type (
    type,
	caption_conf_id,
	model,
	count

)
select
	t.type,
	c.caption_conf_id,
	cc.model as model,
	COUNT(*) as count
from
	core.tree t
left join core.metadata m on
	t.abs_filename = m.abs_filename
join core.caption c on
	m.hash = c.hash
join core.caption_conf cc on
	cc.id = c.caption_conf_id
group by
	t.type,
	c.caption_conf_id,
	cc.model