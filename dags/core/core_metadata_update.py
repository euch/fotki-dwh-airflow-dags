import base64
import json
import os

import psycopg2 as psql
import requests
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, dag, task, Variable

from config import *
from core import DagId


@dag(dag_id=DagId.CORE_METADATA_UPDATE, max_active_runs=1, default_args=dag_args_noretry)
def dag():
    @task
    def add_missing_metadata_collection():
        return _add_missing_metadata(Conn.SMB_COLLECTION, VariableName.STORAGE_PATH_COLLECTION, 'collection')

    @task
    def add_missing_metadata_trash():
        return _add_missing_metadata(Conn.SMB_TRASH, VariableName.STORAGE_PATH_TRASH, 'trash')

    @task
    def add_missing_metadata_archive():
        return _add_missing_metadata(Conn.SMB_ARCHIVE, VariableName.STORAGE_PATH_ARCHIVE, 'archive')

    _add_missing_metadata_collection = add_missing_metadata_collection()
    _add_missing_metadata_trash = add_missing_metadata_trash()
    _add_missing_metadata_archive = add_missing_metadata_archive()

    some_metadata_added = EmptyOperator(task_id='some_metadata_added', outlets=[Asset(AssetName.CORE_UPDATED)])
    none_metadata_added = EmptyOperator(task_id='none_metadata_added', outlets=[])

    @task.branch
    def check_results(result_collection, result_trash, result_archive):
        if result_collection['some_added'] or result_trash['some_added'] or result_archive['some_added']:
            return "some_metadata_added"
        else:
            return "none_metadata_added"

    _choose_branch = check_results(result_collection=_add_missing_metadata_collection,
                                   result_trash=_add_missing_metadata_trash,
                                   result_archive=_add_missing_metadata_archive)

    _choose_branch >> [some_metadata_added, none_metadata_added]


dag()


def _add_missing_metadata(smb_conn_name: str, remote_root_path_varname: str, tree_type: str):
    pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
    smb_hook = SambaHook.get_hook(smb_conn_name)
    remote_root_path = Variable.get(remote_root_path_varname)
    endpoint = Variable.get(VariableName.METADATA_ENDPOINT)
    some_added = False
    corrupt_abs_filenames = {''}

    def result_dict() -> dict:
        return {
            'some_added': some_added,
            'corrupted_files': ','.join(corrupt_abs_filenames)
        }

    while True:
        with open(os.path.join(os.path.dirname(__file__), 'sql', 'metadata_select_missing.sql'), 'r') as f:
            records = pg_hook.get_records(f.read(), parameters=[tree_type, tuple(corrupt_abs_filenames)])

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

                    with open(os.path.join(os.path.dirname(__file__), 'sql', 'metadata_insert.sql'), 'r') as f:
                        pg_hook.run(f.read(), parameters=[abs_filename, resp['hash'], exif, preview])

                    with open(os.path.join(os.path.dirname(__file__), 'sql', 'metadata_update_log.sql'), 'r') as f:
                        pg_hook.run(f.read(), parameters=(resp['hash'], abs_filename))

                    some_added = True
                else:
                    raise AirflowException(
                        f'Helper returned {response.status_code} for {abs_filename}: {response.text}')

            if not has_more_records:
                return result_dict()
