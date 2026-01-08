import hashlib
from datetime import datetime, UTC
from io import BytesIO

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

Y_D_M_H_M = "%Y-%m-%d_%H%M"
HELPER_TIMEOUT_S = 10

class S3InspectImportHook(S3Hook):
    def __init__(self,
                 aws_conn_id: str,
                 pg_conn_id: str,
                 exif_ts_endpoint: str,
                 landing_bucket: str,
                 subfolder_fmt: str,
                 **kwargs):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.s3 = super().get_client_type('s3')
        self.pg_hook = PostgresHook.get_hook(pg_conn_id)
        self.exif_ts_endpoint = exif_ts_endpoint
        self.landing_bucket = landing_bucket
        self.subfolder_fmt = subfolder_fmt

    def create_import_item(self, landing_bucket_key: str) -> dict[str, str]:
        obj_bytes = self._get_s3_object_bytes(landing_bucket_key)
        subfolder = self._get_subfolder(obj_bytes)
        bad_item_dest_key = datetime.now(UTC).strftime(Y_D_M_H_M) + "/" + landing_bucket_key
        if subfolder:
            if self._is_duplicate(obj_bytes):
                return {
                    'landing_bucket_key': landing_bucket_key,
                    'status': 'duplicate',
                    'dest_key': bad_item_dest_key
                }
            else:
                return {
                    'landing_bucket_key': landing_bucket_key,
                    'status': 'good',
                    'dest_subfolder': subfolder,
                    'dest_path': subfolder + "/" + landing_bucket_key.split("/")[-1]
                }
        else:
            return {
                'landing_bucket_key': landing_bucket_key,
                'status': 'unsupported',
                'dest_key': bad_item_dest_key
            }

    def _get_s3_object_bytes(self, landing_bucket_key: str) -> BytesIO:
        resp_obj = self.s3.get_object(Bucket=self.landing_bucket, Key=landing_bucket_key)
        data = resp_obj['Body'].read()
        return BytesIO(data)

    def _get_subfolder(self, obj_bytes: BytesIO) -> str | None:
        files = {'file': ('some_filename', obj_bytes, 'application/octet-stream')}
        data = {'format': self.subfolder_fmt}
        try:
            response = requests.post(self.exif_ts_endpoint, files=files, data=data, timeout=HELPER_TIMEOUT_S)
        except requests.exceptions.Timeout:
            print('The request timed out')
            return None
        if response.status_code == 200:
            metadata = response.json()
            return metadata['timestamp']
        else:
            return None

    def _is_duplicate(self, obj_bytes: BytesIO) -> bool:
        md5_hash = hashlib.file_digest(obj_bytes, "md5").hexdigest()
        records = self.pg_hook.get_records(
            "select 1 "
            "from core.metadata m "
            "join core.tree t on t.abs_filename = m.abs_filename "
            f"where t.\"type\" = 'collection' and m.hash = '{md5_hash}' "
        )
        return len(records) > 0
