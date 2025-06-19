with files_and_types as (
	select
		abs_filename,
		regexp_replace(abs_filename, '^.*[.]([^.]+)$', '\1') as file_type
	from
		core.tree
)
insert into dm.files_and_types
SELECT
  now() as ts,
  file_type,
  count(*) as cnt
FROM
	files_and_types
GROUP BY
	file_type
order by
	cnt desc;
