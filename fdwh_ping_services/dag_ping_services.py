import socket
from datetime import timedelta
from urllib.parse import urlparse

from airflow.models import Variable
from airflow.sdk import asset, Asset

from fdwh_config import VariableName, AssetName

default_args = {
    'retries': 10,
    'retry_delay': 300,
    'retry_exponential_backoff': True
}


@asset(name=AssetName.METADATA_HELPER_AVAIL, schedule=[Asset(AssetName.EDM_TREE_UPDATED)])
def check_metadata() -> bool:
    return check_host_avail(Variable.get(VariableName.METADATA_ENDPOINT))


@asset(name=AssetName.EXIF_TS_HELPER_AVAIL, schedule=timedelta(minutes=10))
def check_exif_ts() -> bool:
    return check_host_avail(Variable.get(VariableName.EXIF_TS_ENDPOINT))


@asset(name=AssetName.AI_DESCR_HELPER_AVAIL, schedule=[Asset(AssetName.EDM_TREE_UPDATED)])
def check_ai_desc() -> bool:
    return check_host_avail(Variable.get(VariableName.AI_DESCR_ENDPOINT))


def check_host_avail(url: str):
    parsed_url = urlparse(url)
    host = parsed_url.hostname
    port = parsed_url.port if parsed_url.port else (80 if parsed_url.scheme == 'http' else 443)
    with socket.create_connection((host, port), timeout=60):
        return True
