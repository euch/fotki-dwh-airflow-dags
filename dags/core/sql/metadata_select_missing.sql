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
	and t.type = %s
	and (m.abs_filename is null --metadata does not exist
		-- or metadata exist, but has empty values
		or m.hash is null
	)
	and t.abs_filename not in %s
order by
    t.abs_filename desc
limit 5