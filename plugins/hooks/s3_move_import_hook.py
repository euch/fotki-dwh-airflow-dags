import shutil
from contextlib import contextmanager
from io import BytesIO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.samba.hooks.samba import SambaHook


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

    def move_s3_import_item(self, import_item: dict[str, str]):
        status = import_item['status']
        landing_bucket_key = import_item['landing_bucket_key']
        if 'good' == status:
            dest_path = import_item['dest_path']
            dest_subfolder = import_item['dest_subfolder']
            self._move_good(dest_path=dest_path, dest_subfolder=dest_subfolder, landing_bucket_key=landing_bucket_key)
        elif 'duplicate' == status:
            dest_key = import_item['dest_key']
            dest_bucket = self.duplicate_bucket
            self._move_bad(dest_key=dest_key, dest_bucket=dest_bucket, landing_bucket_key=landing_bucket_key)
        elif 'unsupported' == status:
            dest_key = import_item['dest_key']
            dest_bucket = self.unsupported_bucket
            self._move_bad(dest_key=dest_key, dest_bucket=dest_bucket, landing_bucket_key=landing_bucket_key)
        else:
            raise NotImplementedError

    def _move_good(self, dest_path, dest_subfolder, landing_bucket_key):
        self.smb_hook_storage.makedirs(dest_subfolder, exist_ok=True)
        with self.smb_hook_storage.open_file(dest_path, mode="wb") as g:
            obj_bytes = self._get_s3_object_bytes(landing_bucket_key)
            shutil.copyfileobj(obj_bytes, g)
            self.s3.delete_object(Bucket=self.landing_bucket, Key=landing_bucket_key)

    def _move_bad(self, dest_key, dest_bucket, landing_bucket_key):
        copy_source = {'Bucket': self.landing_bucket, 'Key': landing_bucket_key}
        self.s3.copy_object(Bucket=dest_bucket, Key=dest_key, CopySource=copy_source)
        self.s3.delete_object(Bucket=self.landing_bucket, Key=landing_bucket_key)

    def _get_s3_object_bytes(self, landing_bucket_key: str) -> BytesIO:
        resp_obj = self.s3.get_object(Bucket=self.landing_bucket, Key=landing_bucket_key)
        data = resp_obj['Body'].read()
        return BytesIO(data)
