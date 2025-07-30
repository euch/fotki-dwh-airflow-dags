import base64
import json
from datetime import datetime

import psycopg2 as psql
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.sdk import Asset, dag, task, Variable
from smbprotocol.exceptions import SMBOSError

from fdwh_config import *
from fdwh_core_metadata.dto.add_metadata_item import AddMetadataItem
from fdwh_op_check_helper_available import CheckHelperAvailableOperator


@dag(max_active_runs=1, default_args=dag_args_retry, schedule=[Asset(AssetName.CORE_TREE_UPDATED)])
def add_missing_metadata():
    assert_metadata_helper_available = CheckHelperAvailableOperator(
        task_id="assert_metadata_helper_available",
        url=Variable.get(VariableName.METADATA_ENDPOINT))

    @task
    def add_missing_metadata_collection() -> list[AddMetadataItem]:
        pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
        smb_hook = SambaHook.get_hook(Conn.SMB_COLLECTION)
        rp = Variable.get(VariableName.RP_COLLECTION)
        results: list[AddMetadataItem] = []
        for abs_filename in _find_missing(pg_hook, 'collection'):
            results.append(_add_missing(smb_hook, pg_hook, rp, abs_filename))
        return results

    @task
    def add_missing_metadata_trash() -> list[AddMetadataItem]:
        pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
        smb_hook = SambaHook.get_hook(Conn.SMB_TRASH)
        rp = Variable.get(VariableName.RP_TRASH)
        results: list[AddMetadataItem] = []
        for abs_filename in _find_missing(pg_hook, 'trash'):
            results.append(_add_missing(smb_hook, pg_hook, rp, abs_filename))
        return results

    @task
    def add_missing_metadata_archive() -> list[AddMetadataItem]:
        pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
        smb_hook = SambaHook.get_hook(Conn.SMB_ARCHIVE)
        rp = Variable.get(VariableName.RP_ARCHIVE)
        results: list[AddMetadataItem] = []
        for abs_filename in _find_missing(pg_hook, 'archive'):
            results.append(_add_missing(smb_hook, pg_hook, rp, abs_filename))
        return results

    @task(outlets=[Asset[AssetName.CORE_METADATA_UPDATED]])
    def end():
        pass

    assert_metadata_helper_available >> add_missing_metadata_collection() >> add_missing_metadata_trash() >> add_missing_metadata_archive() >> end()


add_missing_metadata()

_find_missing_sql = '''
select
    t.abs_filename
from
    core.tree t
where not exists (
    select 1 from core.metadata m
    where m.abs_filename = t.abs_filename
)
and t.size < 1000000000 -- up to 1 GB limit
and t."type" = %s;
'''

_insert_metadata_sql = '''
insert into	
    core.metadata 
    (abs_filename, hash, exif, preview)
values (%s,%s,%s,%s)
'''

_update_log_sql = """
update
    log.core_log 
set 
    metadata_add_ts = %s,
    hash = %s
where
    abs_filename = %s;
"""


def _find_missing(pg_hook, t_type) -> list[str]:
    return list(map(lambda r: r[0], pg_hook.get_records(sql=_find_missing_sql, parameters=[t_type])))


def _add_missing(smb_hook, pg_hook, remote_path: str, abs_filename: str) -> AddMetadataItem:
    try:
        print(f'\n\nadding missing metadata for abs_filename="{abs_filename}"...\n')
        rel_filename = abs_filename.replace(remote_path, '')
        print(f'looking for file {rel_filename}')
        with smb_hook.open_file(path=rel_filename, mode='rb') as file:
            files = {'file': file}
            response = requests.post(Variable.get(VariableName.METADATA_ENDPOINT), files=files)
            print(f'Metadata endpoint responded with code {response.status_code}')
            if response.status_code != 200:
                print(response)
                return AddMetadataItem(abs_filename, error=str(response))
            else:
                resp = response.json()
                parameters = [
                    abs_filename,
                    resp['hash'],
                    None if resp['exif'] is None else json.dumps(resp['exif']),
                    None if resp['preview'] is None else psql.Binary(base64.b64decode(resp['preview']))
                ]
                pg_hook.run(_insert_metadata_sql, parameters=parameters)
                pg_hook.run(_update_log_sql, parameters=(datetime.now(), resp['hash'], abs_filename))
                return AddMetadataItem(abs_filename)
    except SMBOSError as e:
        print(e)
        return AddMetadataItem(abs_filename, error=str(e))
