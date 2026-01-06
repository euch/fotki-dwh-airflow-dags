import shutil
from contextlib import contextmanager

from fdwh_import.dto.import_item import GoodImportItem, DuplicateImportItem, UnrecognizedImportItem, ImportItem


@contextmanager
def ignorednr(exception, *errornrs):
    try:
        yield
    except exception as e:
        if e.errno not in errornrs:
            raise
        pass


def move_s3_import_item(s3, smb_hook_storage, import_item: ImportItem):
    match import_item:
        case GoodImportItem():
            _move_good(import_item, s3, smb_hook_storage)
        case DuplicateImportItem():
            move_duplicate(import_item, s3)
        case UnrecognizedImportItem():
            _move_unrecognized(import_item, s3)


def _move_good(import_item: GoodImportItem, s3, smb_hook_storage):
    smb_hook_storage.makedirs(import_item.storage_dir, exist_ok=True)

    with smb_hook_storage.open_file(import_item.storage_path, mode="wb") as g:
        shutil.copyfileobj(import_item.safe_obj_bytes(), g)

    s3.delete_object(Bucket=import_item.landing_bucket, Key=import_item.landing_bucket_key)


def move_duplicate(import_item: DuplicateImportItem, s3):
    copy_file(
        s3,
        src_key=import_item.landing_bucket_key,
        src_bucket=import_item.landing_bucket,
        dest_key=import_item.duplicate_bucket_key,
        dest_bucket=import_item.duplicate_bucket)

    s3.delete_object(Bucket=import_item.landing_bucket, Key=import_item.landing_bucket_key)


def _move_unrecognized(import_item: UnrecognizedImportItem, s3):
    copy_file(
        s3,
        src_key=import_item.landing_bucket_key,
        src_bucket=import_item.landing_bucket,
        dest_key=import_item.unrecognized_bucket_key,
        dest_bucket=import_item.unrecognized_bucket)

    s3.delete_object(Bucket=import_item.landing_bucket, Key=import_item.landing_bucket_key)


def copy_file(s3, src_key: str, src_bucket, dest_key: str, dest_bucket: str):
    copy_source = {'Bucket': src_bucket, 'Key': src_key}
    s3.copy_object(Bucket=dest_bucket, Key=dest_key, CopySource=copy_source)
