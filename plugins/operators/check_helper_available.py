import socket
from urllib.parse import urlparse

from airflow.models import BaseOperator


class CheckHelperAvailableOperator(BaseOperator):
    def __init__(self, url: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.url = url

    def execute(self, context):
        parsed_url = urlparse(self.url)
        host = parsed_url.hostname
        port = parsed_url.port if parsed_url.port else (80 if parsed_url.scheme == 'http' else 443)
        with socket.create_connection((host, port), timeout=5) as conn:
            conn.close()
            pass

