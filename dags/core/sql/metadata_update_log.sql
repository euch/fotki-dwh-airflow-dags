update
    log.core_log
set
    metadata_add_ts = now(),
    hash = %s
where
    abs_filename = %s