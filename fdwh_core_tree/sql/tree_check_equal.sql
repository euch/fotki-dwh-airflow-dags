select exists (
	select rt.abs_filename from raw.tree_all rt
	except
	select ct.abs_filename from core.tree ct
	--
	union all
    --
	select ct.abs_filename from core.tree ct
	except
    select rt.abs_filename from raw.tree_all rt
) as diff_not_empty;
