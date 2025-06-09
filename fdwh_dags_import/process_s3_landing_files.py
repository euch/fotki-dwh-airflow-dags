import hashlib
import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, UTC
from io import BytesIO

import requests
import smbclient
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.sdk import DAG, Asset
from dataclasses_json import dataclass_json
from smbclient import shutil

from fdwh_config import *

Y_D_M_H_M = "%Y-%d-%m_%H%M"


@contextmanager
def ignorednr(exception, *errornrs):
    try:
        yield
    except exception as e:
        if e.errno not in errornrs:
            raise
        pass


@dataclass_json
@dataclass
class ImportItem:
    landing_bucket: str
    landing_bucket_key: str
    proc_datetime: datetime


@dataclass_json
@dataclass
class GoodImportItem(ImportItem):
    storage_dir: str
    storage_path: str
    status = "import"
    obj_bytes: BytesIO


@dataclass_json
@dataclass
class DuplicateImportItem(ImportItem):
    duplicate_bucket: str
    duplicate_bucket_key: str
    status = "duplicate"


@dataclass_json
@dataclass
class UnrecognizedImportItem(ImportItem):
    unrecognized_bucket: str
    unrecognized_bucket_key: str
    status = "unrecognized"


def import_from_s3_callable():
    import_from_s3(
        landing_bucket=Variable.get(VariableName.BUCKET_LANDING),
        unrecognized_bucket=Variable.get(VariableName.BUCKET_REJECTED_UNSUPPORTED),
        duplicates_bucket=Variable.get(VariableName.BUCKET_REJECTED_DUPLICATES),
        exif_ts_endpoint=Variable.get(VariableName.EXIF_TS_ENDPOINT),
        pg_hook=PostgresHook.get_hook(Conn.POSTGRES),
        s3_hook=S3Hook(aws_conn_id=Conn.MINIO),
        smb_hook_storage=SambaHook.get_hook(Conn.SMB_COLLECTION)
    )


def import_from_s3(pg_hook: PostgresHook, s3_hook: S3Hook,
                   landing_bucket: str, unrecognized_bucket: str, duplicates_bucket: str,
                   exif_ts_endpoint: str, smb_hook_storage: SambaHook):
    s3 = s3_hook.get_client_type('s3')
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=landing_bucket):
        if 'Contents' in page:
            for obj in page['Contents']:
                import_item = create_import_item(
                    s3,
                    pg_hook,
                    landing_bucket_key=obj['Key'],
                    landing_bucket=landing_bucket,
                    exif_ts_endpoint=exif_ts_endpoint,
                    unrecognized_bucket=unrecognized_bucket,
                    duplicate_bucket=duplicates_bucket,
                )
                print(f"ready to move import item: {import_item}")
                move_s3_import_item(s3, smb_hook_storage, import_item)


def create_import_item(s3, pg_hook: PostgresHook, exif_ts_endpoint: str,
                       landing_bucket_key: str,
                       landing_bucket: str, unrecognized_bucket: str, duplicate_bucket: str) -> (ImportItem, BytesIO | None):
    proc_datetime = datetime.now(UTC)

    subfolder, preloaded_obj_bytes = get_subfolder(s3, landing_bucket_key, landing_bucket, exif_ts_endpoint)

    if not subfolder:
        return UnrecognizedImportItem(
            proc_datetime=proc_datetime,
            landing_bucket_key=landing_bucket_key,
            landing_bucket=landing_bucket,
            unrecognized_bucket_key=proc_datetime.strftime(Y_D_M_H_M) + "/" + landing_bucket_key,
            unrecognized_bucket=unrecognized_bucket)

    is_duplicate, preloaded_obj_bytes = run_duplicate_check(
        s3,
        landing_bucket_key=landing_bucket_key,
        landing_bucket=landing_bucket,
        preloaded_obj_bytes=preloaded_obj_bytes,
        pg_hook=pg_hook)

    if is_duplicate:
        return DuplicateImportItem(
            proc_datetime=proc_datetime,
            landing_bucket_key=landing_bucket_key,
            landing_bucket=landing_bucket,
            duplicate_bucket_key=proc_datetime.strftime(Y_D_M_H_M) + "/" + landing_bucket_key,
            duplicate_bucket=duplicate_bucket)

    basename = landing_bucket_key.split("/")[-1]
    if preloaded_obj_bytes:
        obj_bytes = preloaded_obj_bytes
    else:
        obj_bytes = get_s3_object_bytes(s3, bucket=landing_bucket, key=landing_bucket_key)
    return GoodImportItem(
        obj_bytes=obj_bytes,
        proc_datetime=proc_datetime,
        landing_bucket_key=landing_bucket_key,
        landing_bucket=landing_bucket,
        storage_path=subfolder + "/" + basename,
        storage_dir=subfolder,
    )


def run_duplicate_check(s3, landing_bucket_key: str, landing_bucket: str,
                        preloaded_obj_bytes: BytesIO | None, pg_hook: PostgresHook) -> (bool, BytesIO):
    md5_hash, obj_bytes = get_md5_hash(s3, landing_bucket_key, landing_bucket, preloaded_obj_bytes)
    duplicate = hash_already_used(md5_hash, pg_hook)

    return duplicate, obj_bytes


def get_md5_hash(s3, landing_bucket_key: str, landing_bucket: str,
                 preloaded_obj_bytes: BytesIO | None) -> (str, BytesIO):
    if preloaded_obj_bytes:
        obj_bytes = preloaded_obj_bytes
    else:
        obj_bytes = get_s3_object_bytes(s3, landing_bucket_key, landing_bucket)
    md5_hash = hashlib.file_digest(obj_bytes, "md5").hexdigest()
    assert md5_hash
    return md5_hash, obj_bytes


def hash_already_used(md5_hash, pg_hook: PostgresHook) -> bool:
    records = pg_hook.get_records(
        "select 1 "
        "from edm.metadata m "
        "join edm.tree t on t.abs_filename = m.abs_filename "
        f"where t.\"type\" = 'collection' and m.hash = '{md5_hash}' "
    )
    return len(records) > 0


def move_s3_import_item(s3, smb_hook_storage, import_item: ImportItem):
    if isinstance(import_item, UnrecognizedImportItem):
        copy_file(
            s3,
            src_key=import_item.landing_bucket_key,
            src_bucket=import_item.landing_bucket,
            dest_key=import_item.unrecognized_bucket_key,
            dest_bucket=import_item.unrecognized_bucket)
        s3.delete_object(Bucket=import_item.landing_bucket, Key=import_item.landing_bucket_key)
    if isinstance(import_item, DuplicateImportItem):
        copy_file(
            s3,
            src_key=import_item.landing_bucket_key,
            src_bucket=import_item.landing_bucket,
            dest_key=import_item.duplicate_bucket_key,
            dest_bucket=import_item.duplicate_bucket)
        s3.delete_object(Bucket=import_item.landing_bucket, Key=import_item.landing_bucket_key)
    if isinstance(import_item, GoodImportItem):
        smb_hook_storage.makedirs(import_item.storage_dir, exist_ok=True)
        with smb_hook_storage.open_file(import_item.storage_path, mode="wb") as g:
            shutil.copyfileobj(import_item.obj_bytes, g)
        s3.delete_object(Bucket=import_item.landing_bucket, Key=import_item.landing_bucket_key)


def copy_file(s3, src_key: str, src_bucket, dest_key: str, dest_bucket: str):
    copy_source = {'Bucket': src_bucket, 'Key': src_key}
    s3.copy_object(Bucket=dest_bucket, Key=dest_key, CopySource=copy_source)


def get_subfolder(s3, key: str, bucket: str, exif_ts_endpoint: str) -> (str | None, BytesIO | None):
    import_subfolder: str | None = get_subfolder_from_prefix(key)
    if import_subfolder:
        return import_subfolder, None
    else:
        obj_bytes: BytesIO = get_s3_object_bytes(s3, key, bucket)
        import_subfolder: str | None = get_subfolder_from_exif(obj_bytes, exif_ts_endpoint)
        if import_subfolder:
            return import_subfolder, obj_bytes
        else:
            return None, obj_bytes


def get_subfolder_from_prefix(key: str) -> str | None:
    s = key.split("/")
    if len(s) > 0:
        subfolder = s[0]
        if validate_subfolder_fmt(subfolder):
            return subfolder


def validate_subfolder_fmt(s: str) -> bool:
    """Validate context format: 'YYYY-MM-DD description'

    Args:
        s (str): String to validate

    Returns:
        bool: True if format is valid, False otherwise
    """
    # Check general format
    pattern = r"^(\d{4}-\d{2}-\d{2}) (.+)$"
    match = re.match(pattern, s)
    if not match:
        return False

    # Extract date for additional validation
    date_str = match.group(1)

    try:
        # Try to parse date (this will check day/month validity)
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def get_subfolder_from_exif(obj_bytes: BytesIO, exif_ts_endpoint) -> str | None:
    files = {
        'file': ('some_filename', obj_bytes, 'application/octet-stream')
    }
    response = requests.post(exif_ts_endpoint, files=files, data={'format': ImportSettings.TIMESTAMP_FMT})
    assert response.status_code == 200
    metadata = response.json()
    exif_timestamp = metadata['timestamp']
    return exif_timestamp


def get_s3_object_bytes(s3, key: str, bucket: str) -> BytesIO:
    resp_obj = s3.get_object(Bucket=bucket, Key=key)
    data = resp_obj['Body'].read()
    return BytesIO(data)


with DAG(dag_id=DagName.PROCESS_S3_LANDING_FILES, max_active_runs=1,
         schedule=[Asset(AssetName.METADATA_HELPER_AVAIL)], default_args=dag_default_args) as dag:
    process_each_landing_file = PythonOperator(
        task_id="import",
        python_callable=import_from_s3_callable,
        outlets=Asset(AssetName.IMPORT_RESULT)
    ) >> TriggerDagRunOperator(
        task_id="trigger_" + DagName.REFRESH_STORAGE_TREE_INDEX,
        trigger_dag_id=DagName.REFRESH_STORAGE_TREE_INDEX,
        wait_for_completion=False)
