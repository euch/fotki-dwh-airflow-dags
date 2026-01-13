select
	*
from
	raw.tree_all
where
	not exists (select 1 from core.tree where raw.tree_all.abs_filename = core.tree.abs_filename)