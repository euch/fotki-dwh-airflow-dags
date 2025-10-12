from datetime import datetime, UTC
from io import BytesIO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from fdwh_import.dto.import_item import GoodImportItem, ImportItem, UnsupportedImportItem, DuplicateImportItem
from fdwh_import.utils.get_s3_object_bytes import get_s3_object_bytes
from fdwh_import.utils.get_s3_object_size import get_s3_object_size
from fdwh_import.utils.get_subfolder import get_subfolder
from fdwh_import.utils.run_duplicate_check import run_duplicate_check

Y_D_M_H_M = "%Y-%m-%d_%H%M"


def create_import_item(s3: S3Hook,
                       pg_hook: PostgresHook,
                       exif_ts_endpoint: str,
                       landing_bucket_key: str,
                       landing_bucket: str,
                       import_max_file_size: int,
                       unsupported_bucket: str,
                       duplicate_bucket: str) -> (ImportItem, BytesIO | None):
    obj_size: int = get_s3_object_size(s3, key=landing_bucket_key, bucket=landing_bucket)
    if obj_size <= import_max_file_size:
        obj_bytes: BytesIO = get_s3_object_bytes(s3, key=landing_bucket_key, bucket=landing_bucket)
        subfolder = get_subfolder(path=landing_bucket_key, obj_bytes=obj_bytes, exif_ts_endpoint=exif_ts_endpoint)
        if subfolder:
            duplicate = run_duplicate_check(obj_bytes=obj_bytes, pg_hook=pg_hook)
            if duplicate:
                return _create_ii_duplicate(db=duplicate_bucket, lb=landing_bucket, lbk=landing_bucket_key)
            else:
                return _create_ii_good(lb=landing_bucket, lbk=landing_bucket_key, ob=obj_bytes, subfolder=subfolder)
    else:
        return _create_ii_unsupported(lb=landing_bucket, lbk=landing_bucket_key, ub=unsupported_bucket)


def _create_ii_duplicate(db, lb, lbk):
    proc_datetime = datetime.now(UTC)
    return DuplicateImportItem(
        proc_datetime=proc_datetime,
        landing_bucket_key=lbk,
        landing_bucket=lb,
        duplicate_bucket_key=proc_datetime.strftime(Y_D_M_H_M) + "/" + lbk,
        duplicate_bucket=db)


def _create_ii_good(lb, lbk, ob, subfolder):
    basename = lbk.split("/")[-1]
    proc_datetime = datetime.now(UTC)
    return GoodImportItem(
        obj_bytes=ob,
        proc_datetime=proc_datetime,
        landing_bucket_key=lbk,
        landing_bucket=lb,
        storage_path=subfolder + "/" + basename,
        storage_dir=subfolder,
    )


def _create_ii_unsupported(lb, lbk, ub):
    proc_datetime = datetime.now(UTC)
    return UnsupportedImportItem(
        proc_datetime=proc_datetime,
        landing_bucket_key=lbk,
        landing_bucket=lb,
        unsupported_bucket_key=proc_datetime.strftime(Y_D_M_H_M) + "/" + lbk,
        unsupported_bucket=ub)
