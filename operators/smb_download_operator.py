import contextlib
import os

from airflow.models import BaseOperator
from airflow.providers.samba.hooks.samba import SambaHook


class SmbDownloadOperator(BaseOperator):
    def __init__(self, smb_conn_name: str, remote_path: str, local_path: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.smb_conn_name = smb_conn_name
        self.remote_path = remote_path
        self.local_path = local_path

    def execute(self, context):
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.local_path)
        smb_hook = SambaHook.get_hook(self.smb_conn_name)
        with smb_hook.open_file(path=self.remote_path, mode='r', share_access='r') as remote_csv:
            with open(self.local_path, "w") as local_csv:
                local_csv.truncate()
                local_csv.write(remote_csv.read())
                local_csv.close()
            remote_csv.close()
