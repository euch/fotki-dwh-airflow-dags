insert into	dm.total_count_by_type (
    type,
	count
)
select
	t.type,
	COUNT(*) as count
from
	core.tree t
group by
	t.type
