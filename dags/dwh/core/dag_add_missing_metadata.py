import base64
import json

import psycopg2 as psql
import requests
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.sdk import Asset, dag, task, Variable

from config import *
from operators.check_helper_available import CheckHelperAvailableOperator

schedule = [Asset(AssetName.CORE_TREE_UPDATED)]
tags = {
    DagTag.DWH_CORE,
    DagTag.HELPERS,
    DagTag.STORAGE_IO,
    DagTag.PG,
    DagTag.SMB,
}


@dag(dag_id=DagName.ADD_MISSING_METADATA, max_active_runs=1, default_args=dag_args_retry, schedule=schedule, tags=tags)
def add_missing_metadata():
    assert_metadata_helper_available = CheckHelperAvailableOperator(
        task_id="assert_metadata_helper_available",
        url=Variable.get(VariableName.METADATA_ENDPOINT))

    @task
    def add_missing_metadata_collection():
        return _add_missing_metadata(Conn.SMB_COLLECTION, VariableName.STORAGE_PATH_COLLECTION, 'collection')

    @task
    def add_missing_metadata_trash():
        return _add_missing_metadata(Conn.SMB_TRASH, VariableName.STORAGE_PATH_TRASH, 'trash')

    @task
    def add_missing_metadata_archive():
        return _add_missing_metadata(Conn.SMB_ARCHIVE, VariableName.STORAGE_PATH_ARCHIVE, 'archive')

    @task(outlets=[Asset[AssetName.CORE_METADATA_UPDATED]])
    def end():
        pass

    (assert_metadata_helper_available >> [
        add_missing_metadata_collection(),
        add_missing_metadata_trash(),
        add_missing_metadata_archive()
    ] >> end())


add_missing_metadata()

missing_metadata_select_sql = '''
select
	t.abs_filename,
    CASE 
        WHEN COUNT(*) OVER() >= 5 THEN true 
        ELSE false 
    END as has_more_pages
from
	core.tree t
left join core.metadata m on
	m.abs_filename = t.abs_filename
where
	t.size < 1000000000
	-- up to 1 GB limit
	and t.type = %s
	and (m.abs_filename is null --metadata does not exist 
		-- or metadata exist, but has empty values 
		or m.hash is null
	)
	and t.abs_filename not in %s
order by
    t.abs_filename desc
limit 5
;
'''

_insert_metadata_sql = '''
insert
	into core.metadata 
    (abs_filename, hash, exif, preview)
values (%s,%s,%s,%s)
on conflict (abs_filename) do
update
set
	hash = EXCLUDED.hash,
	exif = EXCLUDED.exif,
	preview = EXCLUDED.preview
where
	core.metadata.hash is distinct from	EXCLUDED.hash
	or core.metadata.exif::text is distinct from EXCLUDED.exif::text
	or core.metadata.preview is distinct from EXCLUDED.preview;
'''

_update_log_sql = """
update
    log.core_log 
set 
    metadata_add_ts = now(),
    hash = %s
where
    abs_filename = %s;
"""


def _add_missing_metadata(smb_conn_name: str, remote_root_path_varname: str, tree_type: str):
    pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
    smb_hook = SambaHook.get_hook(smb_conn_name)
    remote_root_path = Variable.get(remote_root_path_varname)
    endpoint = Variable.get(VariableName.METADATA_ENDPOINT)
    corrupt_abs_filenames = {''}

    def result_dict() -> dict:
        return {
            'corrupted_files': ','.join(corrupt_abs_filenames)
        }

    while True:
        records = pg_hook.get_records(missing_metadata_select_sql, parameters=[tree_type, tuple(corrupt_abs_filenames)])

        if not records:
            return result_dict()

        for r in records:
            abs_filename, has_more_records = r[0], r[1]
            rel_filename = abs_filename.replace(remote_root_path, '')

            with smb_hook.open_file(path=rel_filename, mode='rb', share_access='r') as file:
                response = requests.post(endpoint, files={'file': file}, data={'filename': abs_filename})
                print(f'Metadata endpoint responded with code {response.status_code}')

                if response.status_code == 200:
                    resp = response.json()

                    exif = None if resp['exif'] is None else json.dumps(resp['exif'])
                    preview = None if resp['preview'] is None else psql.Binary(base64.b64decode(resp['preview']))

                    if not exif or not preview:
                        corrupt_abs_filenames.add(abs_filename)

                    pg_hook.run(_insert_metadata_sql, parameters=[abs_filename, resp['hash'], exif, preview])
                    pg_hook.run(_update_log_sql, parameters=(resp['hash'], abs_filename))

                else:
                    raise AirflowException(
                        f'Helper returned {response.status_code} for {abs_filename}: {response.text}')

            if not has_more_records:
                return result_dict()
