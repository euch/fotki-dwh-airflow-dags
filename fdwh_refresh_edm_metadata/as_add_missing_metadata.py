import base64
import hashlib
import json
from datetime import datetime

import psycopg2 as psql
import requests
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.sdk import asset, Asset
from smbprotocol.exceptions import SMBOSError

from fdwh_config import Conn, VariableName, AssetName


@asset(name=AssetName.ADD_METADATA_ARCHIVE, schedule=[
    Asset(AssetName.METADATA_HELPER_AVAIL),
    Asset(AssetName.MISSING_METADATA_ARCHIVE)
])
def add_metadata_archive(context: dict, fdwh_metadata_helper_avail: Asset, fdwh_missing_ma: Asset) -> None:
    avail: bool = context["inlet_events"][fdwh_metadata_helper_avail][-1].source_task_instance.xcom_pull()
    if avail:
        items = context["inlet_events"][fdwh_missing_ma][-1].source_task_instance.xcom_pull()
        if len(items) > 0:
            add_missing_metadata(Conn.SMB_ARCHIVE, Variable.get(VariableName.RP_ARCHIVE), items)


@asset(name=AssetName.ADD_METADATA_COLLECTION, schedule=[
    Asset(AssetName.METADATA_HELPER_AVAIL),
    Asset(AssetName.MISSING_METADATA_COLLECTION)
])
def add_metadata_collection(context: dict, fdwh_metadata_helper_avail: Asset, fdwh_missing_mc: Asset) -> None:
    avail: bool = context["inlet_events"][fdwh_metadata_helper_avail][-1].source_task_instance.xcom_pull()
    if avail:
        items = context["inlet_events"][fdwh_missing_mc][-1].source_task_instance.xcom_pull()
        if len(items) > 0:
            add_missing_metadata(Conn.SMB_COLLECTION, Variable.get(VariableName.RP_COLLECTION), items)


@asset(name=AssetName.ADD_METADATA_TRASH, schedule=[
    Asset(AssetName.METADATA_HELPER_AVAIL),
    Asset(AssetName.MISSING_METADATA_TRASH)
])
def add_metadata_trash(context: dict, fdwh_metadata_helper_avail: Asset, fdwh_missing_mt: Asset) -> None:
    avail: bool = context["inlet_events"][fdwh_metadata_helper_avail][-1].source_task_instance.xcom_pull()
    if avail:
        items = context["inlet_events"][fdwh_missing_mt][-1].source_task_instance.xcom_pull()
        if len(items) > 0:
            add_missing_metadata(Conn.SMB_TRASH, Variable.get(VariableName.RP_TRASH), items)


def add_missing_metadata(smb_conn_name: str, remote_path: str, items):
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
                assert response.status_code == 200
                metadata = response.json()

                if metadata['preview'] is not None:
                    preview_bytes = base64.b64decode(metadata['preview'])

                hashsum = hashlib.file_digest(file, "md5").hexdigest()
                print(f'hashsum = {hashsum}')

                pg_hook.run("insert into edm.metadata (abs_filename, hash, exif, preview) values (%s,%s,%s,%s)",
                            parameters=(abs_filename,
                                        hashsum,
                                        None if metadata['exif'] is None else json.dumps(metadata['exif']),
                                        None if preview_bytes is None else psql.Binary(preview_bytes)))

                pg_hook.run(
                    """
                    update
                        log.edm_log 
                    set 
                        metadata_add_ts = %s,
                        hash = %s
                    where
                        abs_filename = %s;
                    """, parameters=(datetime.now(), hashsum, abs_filename))
        except SMBOSError as smb_err:
            print(smb_err)
