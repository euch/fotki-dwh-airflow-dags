import socket
from urllib.parse import urlparse


def check_http(url: str):
    parsed_url = urlparse(url)
    host = parsed_url.hostname
    port = parsed_url.port if parsed_url.port else (80 if parsed_url.scheme == 'http' else 443)
    with socket.create_connection((host, port), timeout=5) as conn:
        conn.close()
        pass