import socket
from datetime import timedelta
from urllib.parse import urlparse

from airflow.models import Variable
from airflow.sdk import task, Asset, dag

from fdwh_config import VariableName, AssetName, DagName, dag_default_args


@dag(dag_id=DagName.CHECK_HELPER_SERVICES, max_active_runs=1, schedule=timedelta(minutes=1),
     default_args=dag_default_args)
def check_service_avail():
    @task(outlets=[Asset(AssetName.METADATA_HELPER_AVAIL)])
    def check_metadata_helper():
        return check_host_avail(Variable.get(VariableName.METADATA_ENDPOINT))

    @task(outlets=[Asset(AssetName.AI_DESCR_HELPER_AVAIL)])
    def check_ai_desc_helper():
        return check_host_avail(Variable.get(VariableName.AI_DESCR_ENDPOINT))

    [check_ai_desc_helper(), check_metadata_helper()]


check_service_avail()


def check_host_avail(url: str):
    parsed_url = urlparse(url)
    host = parsed_url.hostname
    port = parsed_url.port if parsed_url.port else (80 if parsed_url.scheme == 'http' else 443)
    with socket.create_connection((host, port), timeout=60):
        return True


# Using the special variable
# __name__
if __name__ == "__main__":
    print(check_host_avail("http://192.168.1.120:5000/metadata"))
