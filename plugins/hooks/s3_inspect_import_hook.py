import hashlib
from datetime import datetime, UTC
from io import BytesIO

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from config import ImportSettings
from dto.s3_import.import_item import GoodImportItem, ImportItem, UnsupportedImportItem, DuplicateImportItem

Y_D_M_H_M = "%Y-%m-%d_%H%M"


class S3InspectImportHook(S3Hook):
    def __init__(self,
                 aws_conn_id: str,
                 pg_conn_id: str,
                 exif_ts_endpoint: str,
                 landing_bucket: str,
                 **kwargs):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.s3 = super().get_client_type('s3')
        self.pg_hook = PostgresHook.get_hook(pg_conn_id)
        self.exif_ts_endpoint = exif_ts_endpoint
        self.landing_bucket = landing_bucket

    def create_import_item(self, landing_bucket_key) -> ImportItem:
        obj_bytes = self._get_s3_object_bytes()
        subfolder = self._get_subfolder(obj_bytes)
        bad_item_dest_key = datetime.now(UTC).strftime(Y_D_M_H_M) + "/" + self.landing_bucket_key
        if subfolder:
            if self._is_duplicate(obj_bytes):
                return DuplicateImportItem(landing_bucket_key=self.landing_bucket_key, dest_key=bad_item_dest_key)
            else:
                return GoodImportItem(obj_bytes, subfolder)
        else:
            return UnsupportedImportItem(landing_bucket_key=self.landing_bucket_key, dest_key=bad_item_dest_key)

    def _get_s3_object_bytes(self) -> BytesIO:
        resp_obj = self.s3.get_object(Bucket=self.landing_bucket, Key=self.landing_bucket_key)
        data = resp_obj['Body'].read()
        return BytesIO(data)

    def _get_subfolder(self, obj_bytes: BytesIO) -> str | None:
        files = {'file': ('some_filename', obj_bytes, 'application/octet-stream')}
        data = {'format': ImportSettings.TIMESTAMP_FMT}
        response = requests.post(self.exif_ts_endpoint, files=files, data=data)
        assert response.status_code == 200
        metadata = response.json()
        return metadata['timestamp']

    def _is_duplicate(self, obj_bytes: BytesIO) -> bool:
        md5_hash = hashlib.file_digest(obj_bytes, "md5").hexdigest()
        records = self.pg_hook.get_records(
            "select 1 "
            "from core.metadata m "
            "join core.tree t on t.abs_filename = m.abs_filename "
            f"where t.\"type\" = 'collection' and m.hash = '{md5_hash}' "
        )
        return len(records) > 0
