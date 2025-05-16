begin;
create temporary table old_files as (
  select
    edm.tree.abs_filename
  from
    edm.tree
    left join raw.tree_all on raw.tree_all.abs_filename = edm.tree.abs_filename
  where
    raw.tree_all.abs_filename is null
);
delete from
  edm.tree
where
  exists (
    select
      1
    from
      old_files
    where
      old_files.abs_filename = edm.tree.abs_filename
  );
insert into log.edm_deleted_log (
  abs_filename,
  hash,
  tree_del_ts,
  tree_add_ts,
  metadata_add_ts,
  ai_description_add_ts
)
select
  abs_filename,
  hash,
  now(),
  tree_add_ts,
  metadata_add_ts,
  ai_description_add_ts
from
  log.edm_log
where
  exists (
    select
      1
    from
      old_files
    where
      old_files.abs_filename = log.edm_log.abs_filename
  );
delete from
  log.edm_log
where
  exists (
    select
      1
    from
      old_files
    where
      old_files.abs_filename = log.edm_log.abs_filename
  );
drop
  table old_files;
commit;
