
insert into duplicates.collection_duplicates
with dup_hash as (
    select hash, count(*) as cnt from core.metadata m
    join core.tree t on t.abs_filename = m.abs_filename
	where t."type" = 'collection'
	group by hash
	having count(*) > 1
)
select m.abs_filename, m.hash, m.preview, dup_hash.cnt, false as "delete" from core.metadata m
inner join dup_hash on dup_hash.hash = m.hash
order by dup_hash.cnt desc, m.hash, m.abs_filename;