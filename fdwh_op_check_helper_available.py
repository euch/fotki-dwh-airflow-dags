import socket
from urllib.parse import urlparse

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator


class CheckHelperAvailableOperator(BaseOperator):
    def __init__(self, url: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.url = url

    def execute(self, context):
        parsed_url = urlparse(self.url)
        host = parsed_url.hostname
        port = parsed_url.port if parsed_url.port else (80 if parsed_url.scheme == 'http' else 443)
        try:
            with socket.create_connection((host, port), timeout=5) as conn:
                conn.close()
                pass
        except (OSError, TimeoutError) as e:
            if '[Errno 113]' in str(e):
                raise AirflowSkipException("No route to host") from e
            if 'timed out' in str(e):
                raise AirflowSkipException("Host available, connection to port timed out") from e
            raise e

