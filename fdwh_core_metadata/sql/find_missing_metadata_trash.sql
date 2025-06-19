select
    t.abs_filename
from
    core.tree t
where not exists (
    select 1 from core.metadata m
    where m.abs_filename = t.abs_filename
)
and t.size < 1000000000 -- up to 1 GB limit
and t."type" = 'trash';