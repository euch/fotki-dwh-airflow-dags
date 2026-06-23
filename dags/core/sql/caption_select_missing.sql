select
	distinct on
	(m.hash)
    m.hash,
	m.preview,
	m.abs_filename,
	case
		when count(*) over() >= 5 then true
		else false
	end as has_more_pages
from
	core.metadata m
left join core.caption c on
	c.hash = m.hash
where
	c.hash is null
	and m.preview is not null
order by
	m.hash,
	m.abs_filename desc
limit 5