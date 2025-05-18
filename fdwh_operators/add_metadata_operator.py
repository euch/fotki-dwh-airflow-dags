import base64
import hashlib
import json
from datetime import datetime

import psycopg2 as psql
import requests
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.samba.hooks.samba import SambaHook
from smbprotocol.exceptions import SMBOSError


class AddMetadataOperator(BaseOperator):
    def __init__(self,
                 xcom_key: str,
                 pg_conn_name: str,
                 smb_conn_name: str,
                 metadata_endpoint: str,
                 filename_converter_func,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.xcom_key = xcom_key
        self.pg_conn_name = pg_conn_name
        self.smb_conn_name = smb_conn_name
        self.metadata_endpoint = metadata_endpoint
        self.filename_converter_func = filename_converter_func

    def execute(self, context):
        items: list[str] = context['task_instance'].xcom_pull(dag_id=self.dag_id,
                                                              task_ids=self.xcom_key,
                                                              key='return_value')
        pg_hook = PostgresHook.get_hook(self.pg_conn_name)
        smb_hook = SambaHook.get_hook(self.smb_conn_name)
        for item in items:
            abs_filename = item[0]
            rel_filename = self.filename_converter_func(abs_filename)
            try:
                print(f'looking for file {rel_filename}')
                with smb_hook.open_file(path=rel_filename, mode='rb') as file:

                    files = {'file': file}
                    response = requests.post(self.metadata_endpoint, files=files)
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
