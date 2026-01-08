import shutil
from contextlib import contextmanager

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.samba.hooks.samba import SambaHook

from dto.s3_import.import_item import GoodImportItem, DuplicateImportItem, UnsupportedImportItem, ImportItem, \
    BadImportItem


class S3MoveImportHook(S3Hook):
    def __init__(self, aws_conn_id: str, smb_conn_id: str, landing_bucket: str, duplicate_bucket: str,
                 unsupported_bucket: str,
                 **kwargs):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.s3 = super().get_client_type('s3')
        self.landing_bucket = landing_bucket
        self.duplicate_bucket = duplicate_bucket
        self.unsupported_bucket = unsupported_bucket
        self.smb_hook_storage = SambaHook.get_hook(smb_conn_id)

    @contextmanager
    def ignorednr(exception, *errornrs):
        try:
            yield
        except exception as e:
            if e.errno not in errornrs:
                raise
            pass

    def move_s3_import_item(self, import_item: ImportItem):
        if isinstance(import_item, GoodImportItem):
            self._move_good(import_item)
        elif isinstance(import_item, BadImportItem):
            self._move_bad(import_item)
        else:
            raise NotImplementedError

    def _move_good(self, import_item):
        self.smb_hook_storage.makedirs(import_item.storage_dir, exist_ok=True)
        with self.smb_hook_storage.open_file(import_item.storage_path, mode="wb") as g:
            shutil.copyfileobj(import_item.safe_obj_bytes(), g)
            self.s3.delete_object(Bucket=self.landing_bucket, Key=import_item.landing_bucket_key)

    def _move_bad(self, import_item):
        dest_bucket = self._bad_item_dest_bucket(import_item)
        copy_source = {'Bucket': self.landing_bucket, 'Key': import_item.landing_bucket_key}
        self.s3.copy_object(Bucket=dest_bucket, Key=import_item.dest_key, CopySource=copy_source)
        self.s3.delete_object(Bucket=self.landing_bucket, Key=import_item.landing_bucket_key)

    def _bad_item_dest_bucket(self, import_item) -> str:
        if isinstance(import_item, DuplicateImportItem):
            return self.duplicate_bucket
        elif isinstance(import_item, UnsupportedImportItem):
            return self.unsupported_bucket
        else:
            raise NotImplementedError
