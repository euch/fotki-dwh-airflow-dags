from datetime import timedelta

from airflow.sdk import dag
from airflow.sdk.definitions.decorators import task
from airflow.timetables.trigger import DeltaTriggerTimetable

from check import check_http
from variables import ollama_endpoint, metadata_endpoint


@dag(max_active_runs=1, default_args={'retries': 0}, schedule=DeltaTriggerTimetable(timedelta(hours=1)))
def service_check():
    @task
    def check_metadata():
        check_http(metadata_endpoint())

    @task
    def check_ollama():
        check_http(ollama_endpoint())

    check_metadata()
    check_ollama()


service_check()
