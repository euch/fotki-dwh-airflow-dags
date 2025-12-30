from datetime import datetime, UTC
from io import BytesIO

from airflow.providers.postgres.hooks.postgres import PostgresHook

from s3_import.dto.import_item import GoodImportItem, ImportItem, UnrecognizedImportItem, DuplicateImportItem
from s3_import.utils.get_s3_object_bytes import get_s3_object_bytes
from s3_import.utils.get_subfolder import get_subfolder
from s3_import.utils.run_duplicate_check import run_duplicate_check

Y_D_M_H_M = "%Y-%m-%d_%H%M"


def create_import_item(s3, pg_hook: PostgresHook, exif_ts_endpoint: str, landing_bucket_key: str, landing_bucket: str,
                       unrecognized_bucket: str, duplicate_bucket: str) -> (ImportItem, BytesIO | None):
    subfolder, preloaded_obj_bytes = get_subfolder(s3, landing_bucket_key, landing_bucket, exif_ts_endpoint)
    proc_datetime = datetime.now(UTC)

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
