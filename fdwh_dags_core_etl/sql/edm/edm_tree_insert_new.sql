begin;
create temporary table new_files as (
  select
    *
  from
    raw.tree_all
  where
    not exists (
      select
        1
      from
        edm.tree
      where
        raw.tree_all.abs_filename = edm.tree.abs_filename
    )
);
insert into edm.tree (
  abs_filename, last_modified_ts, "size",
  "type"
)
select
  abs_filename,
  last_modified_ts,
  "size",
  "type"
from
  new_files;
insert into log.edm_log (abs_filename, tree_add_ts)
select
  abs_filename,
  now()
from
  new_files;
drop
  table new_files;
commit;
