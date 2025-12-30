insert
	into
	dm.preview_count_by_type (
    type,
	count
)
select
	t.type,
	COUNT(*) as count
from
	core.tree t
join core.metadata m on
	m.abs_filename = t.abs_filename
where
	m.preview is not null
group by
	t.type