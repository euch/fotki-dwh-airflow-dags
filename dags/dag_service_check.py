import socket
from datetime import timedelta
from urllib.parse import urlparse

from airflow.sdk import Variable, dag
from airflow.sdk.definitions.decorators import task
from airflow.timetables.trigger import DeltaTriggerTimetable

from config import *

schedule = DeltaTriggerTimetable(timedelta(hours=1))


def check_http(url: str):
    parsed_url = urlparse(url)
    host = parsed_url.hostname
    port = parsed_url.port if parsed_url.port else (80 if parsed_url.scheme == 'http' else 443)
    with socket.create_connection((host, port), timeout=5) as conn:
        conn.close()
        pass


@dag(max_active_runs=1, default_args=dag_args_noretry, schedule=schedule)
def service_check():
    @task
    def check_metadata():
        check_http(Variable.get(VariableName.METADATA_ENDPOINT))

    @task
    def check_exif_ts():
        check_http(Variable.get(VariableName.EXIF_TS_ENDPOINT))

    @task
    def check_ollama():
        check_http(Variable.get(VariableName.OLLAMA_ENDPOINT))

    check_metadata()
    check_exif_ts()
    check_ollama()


service_check()
