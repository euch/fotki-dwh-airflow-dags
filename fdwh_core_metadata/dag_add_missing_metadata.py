import base64
import hashlib
import json
from datetime import datetime

import psycopg2 as psql
import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.sdk import Asset, dag, task, Variable
from smbprotocol.exceptions import SMBOSError

from fdwh_config import Conn, VariableName, AssetName, dag_default_args
from fdwh_op_check_helper_available import CheckHelperAvailableOperator

default_args = dag_default_args | {'dag_concurrency': 1}


@dag(max_active_runs=1, default_args=default_args, schedule=[Asset(AssetName.CORE_TREE_UPDATED)])
def add_missing_metadata():
    assert_metadata_helper_available = CheckHelperAvailableOperator(
        task_id="assert_metadata_helper_available",
        url=Variable.get(VariableName.METADATA_ENDPOINT),
        outlets=[Asset(AssetName.METADATA_HELPER_AVAIL)])

    find_missing_metadata_archive = SQLExecuteQueryOperator(
        task_id='find_missing_metadata_archive',
        conn_id=Conn.POSTGRES,
        sql='sql/find_missing_metadata_archive.sql',
        do_xcom_push=True)

    @task(outlets=[Asset(AssetName.METADATA_UPDATED_ARCHIVE)])
    def add_missing_metadata_archive(**context):
        missing_items = context["ti"].xcom_pull(task_ids="find_missing_metadata_archive", key="return_value")
        if len(missing_items) > 0:
            _add_missing_metadata(Conn.SMB_ARCHIVE, Variable.get(VariableName.RP_ARCHIVE), missing_items)

    assert_metadata_helper_available >> find_missing_metadata_archive >> add_missing_metadata_archive()

    find_missing_metadata_collection = SQLExecuteQueryOperator(
        task_id='find_missing_metadata_collection',
        conn_id=Conn.POSTGRES,
        sql='sql/find_missing_metadata_collection.sql',
        do_xcom_push=True)

    @task(outlets=[Asset(AssetName.METADATA_UPDATED_COLLECTION)])
    def add_missing_metadata_collection(**context):
        missing_items = context["ti"].xcom_pull(task_ids="find_missing_metadata_collection", key="return_value")
        if len(missing_items) > 0:
            _add_missing_metadata(Conn.SMB_COLLECTION, Variable.get(VariableName.RP_COLLECTION), missing_items)

    assert_metadata_helper_available >> find_missing_metadata_collection >> add_missing_metadata_collection()

    find_missing_metadata_trash = SQLExecuteQueryOperator(
        task_id='find_missing_metadata_trash',
        conn_id=Conn.POSTGRES,
        sql='sql/find_missing_metadata_trash.sql',
        do_xcom_push=True)

    @task(outlets=[Asset(AssetName.METADATA_UPDATED_TRASH)])
    def add_missing_metadata_trash(**context):
        missing_items = context["ti"].xcom_pull(task_ids="find_missing_metadata_trash", key="return_value")
        if len(missing_items) > 0:
            _add_missing_metadata(Conn.SMB_TRASH, Variable.get(VariableName.RP_TRASH), missing_items)

    assert_metadata_helper_available >> find_missing_metadata_trash >> add_missing_metadata_trash()


add_missing_metadata()


def _add_missing_metadata(smb_conn_name: str, remote_path: str, items):
    pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
    smb_hook = SambaHook.get_hook(smb_conn_name)
    for item in items:
        abs_filename = item[0]
        rel_filename = abs_filename.replace(remote_path, '')
        try:
            print(f'looking for file {rel_filename}')
            with smb_hook.open_file(path=rel_filename, mode='rb') as file:

                files = {'file': file}
                response = requests.post(Variable.get(VariableName.METADATA_ENDPOINT), files=files)
                print(f'Metadata endpoint responded with code {response.status_code}')
                if response.status_code != 200:
                    print(response)
                else:
                    metadata = response.json()

                    if metadata['preview'] is not None:
                        preview_bytes = base64.b64decode(metadata['preview'])

                    hashsum = hashlib.file_digest(file, "md5").hexdigest()
                    print(f'hashsum = {hashsum}')

                    pg_hook.run("insert into core.metadata (abs_filename, hash, exif, preview) values (%s,%s,%s,%s)",
                                parameters=(abs_filename,
                                            hashsum,
                                            None if metadata['exif'] is None else json.dumps(metadata['exif']),
                                            None if preview_bytes is None else psql.Binary(preview_bytes)))

                    pg_hook.run(
                        """
                        update
                            log.core_log 
                        set 
                            metadata_add_ts = %s,
                            hash = %s
                        where
                            abs_filename = %s;
                        """, parameters=(datetime.now(), hashsum, abs_filename))
        except SMBOSError as smb_err:
            print(smb_err)
