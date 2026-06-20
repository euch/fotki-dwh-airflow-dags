select
    distinct on (m.hash)
    m.hash,
    m.preview,
    m.abs_filename,
    CASE
        WHEN COUNT(*) OVER() >= 5 THEN true
        ELSE false
    END as has_more_pages
from
    core.metadata m
left join core.caption c on
    c.hash = m.hash
join core.tree t on
    t.abs_filename = m.abs_filename
where
    c.hash is null
    and m.preview is not null
    and t."type" = %s
order by
    m.hash, m.abs_filename desc
limit 5