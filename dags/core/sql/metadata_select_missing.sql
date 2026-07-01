select
	t.abs_filename,
    CASE
        WHEN COUNT(*) OVER() >= 5 THEN true
        ELSE false
    END as has_more_pages
from
	core.tree t
left join core.metadata m on
	m.abs_filename = t.abs_filename
where
	t.size < 1000000000
	-- up to 1 GB limit
	and m.abs_filename is null
	-- core.metadata record missing
	and NOT (t.abs_filename = ANY(%s::varchar[]))
	-- ignore specified core.tree records
order by
    t.abs_filename desc
limit 5