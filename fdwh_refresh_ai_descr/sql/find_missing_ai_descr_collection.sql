select
	t.abs_filename
from
	edm.tree m
join edm.tree t on
	t.abs_filename = m.abs_filename
where
	not exists (
        select
            1
        from
            edm.ai_description d
        where
            m.abs_filename = d.abs_filename
    )
	and t."type" = 'collection'
order by t.abs_filename asc;