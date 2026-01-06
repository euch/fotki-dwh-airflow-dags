from datetime import timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Variable, dag
from airflow.timetables.trigger import DeltaTriggerTimetable
from airflow.utils.trigger_rule import TriggerRule

from config import *
from operators.check_bucket_available import CheckBucketHelperAvailableOperator
from operators.check_helper_available import CheckHelperAvailableOperator

schedule = DeltaTriggerTimetable(timedelta(hours=1))
tags = {
    DagTag.HELPERS,
    DagTag.MONITORING,
}


@dag(dag_id=DagName.PING_SERVICES, max_active_runs=1, default_args=dag_args_noretry, schedule=schedule, tags=tags)
def dag():
    finish = EmptyOperator(task_id='finish', trigger_rule=TriggerRule.ALL_DONE)

    CheckHelperAvailableOperator(
        task_id="check_metadata",
        url=Variable.get(VariableName.METADATA_ENDPOINT)) >> finish

    CheckHelperAvailableOperator(
        task_id="check_exif_ts",
        url=Variable.get(VariableName.EXIF_TS_ENDPOINT)) >> finish

    CheckHelperAvailableOperator(
        task_id="check_ollama",
        url=Variable.get(VariableName.OLLAMA_ENDPOINT)) >> finish

    for var_name in VariableName.BUCKETS:
        bucket_name = Variable.get(var_name)
        CheckBucketHelperAvailableOperator(
            task_id=f'check_bucket_{bucket_name}',
            connection=Conn.MINIO,
            bucket_name=bucket_name)


dag()
