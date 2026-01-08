from datetime import timedelta

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Asset, dag, Variable, task, TriggerRule, task_group
from airflow.timetables.trigger import DeltaTriggerTimetable

from config import *
from dto.s3_import.import_item import ImportItem
from hooks.s3_inspect_import_hook import S3InspectImportHook
from hooks.s3_move_import_hook import S3MoveImportHook
from operators.check_helper_available import CheckHelperAvailableOperator

schedule = DeltaTriggerTimetable(timedelta(minutes=1))
tags = {
    DagTag.HELPERS,
    DagTag.STORAGE_IO,
    DagTag.PG,
    DagTag.S3,
    DagTag.SMB,
}


@dag(dag_id=DagName.IMPORT_LANDING_FILES_S3, max_active_runs=1, default_args=dag_args_noretry, schedule=schedule,
     tags=tags)
def dag():
    finish = EmptyOperator(task_id="finish", trigger_rule=TriggerRule.ONE_SUCCESS)

    wait_for_any_s3_file = S3KeySensor(
        task_id='wait_for_any_s3_file',
        bucket_key='*',
        bucket_name=Variable.get(VariableName.BUCKET_LANDING),
        wildcard_match=True,
        poke_interval=60,  # Check every 60 seconds
        timeout=21600,  # Timeout after 6 hour
        aws_conn_id=Conn.MINIO,
        soft_fail=True,
    )

    wait_timeout = EmptyOperator(task_id="wait_timeout", trigger_rule=TriggerRule.ALL_FAILED)
    wait_success = EmptyOperator(task_id="wait_success")

    wait_for_any_s3_file >> [wait_timeout, wait_success]
    wait_timeout >> finish

    assert_exif_helper_available = CheckHelperAvailableOperator(
        task_id="assert_exif_helper_available",
        url=Variable.get(VariableName.EXIF_TS_ENDPOINT))

    @task
    def s3_list() -> list[str]:
        s3_hook = S3Hook(aws_conn_id=Conn.MINIO)
        return s3_hook.list_keys(bucket_name=Variable.get(VariableName.BUCKET_LANDING), max_items=64)

    _s3_list = s3_list()

    wait_success >> assert_exif_helper_available >> _s3_list

    @task_group
    def s3_import(landing_bucket_key: str):
        @task(max_active_tis_per_dag=4)
        def inspect() -> ImportItem:
            hook = S3InspectImportHook(
                aws_conn_id=Conn.MINIO,
                pg_conn_id=Conn.POSTGRES,
                exif_ts_endpoint=Variable.get(VariableName.EXIF_TS_ENDPOINT),
                landing_bucket=Variable.get(VariableName.BUCKET_LANDING))
            return hook.create_import_item(landing_bucket_key)

        @task(max_active_tis_per_dag=4)
        def move(import_item: ImportItem):
            hook = S3MoveImportHook(
                aws_conn_id=Conn.MINIO,
                smb_conn_id=Conn.SMB_COLLECTION,
                landing_bucket=Variable.get(VariableName.BUCKET_LANDING),
                unsupported_bucket=Variable.get(VariableName.BUCKET_REJECTED_UNSUPPORTED),
                duplicate_bucket=Variable.get(VariableName.BUCKET_REJECTED_DUPLICATES))
            hook.move_s3_import_item(import_item)

    _s3_import = s3_import.expand(key=_s3_list)

    import_sync = EmptyOperator(task_id="import_sync", trigger_rule=TriggerRule.ALL_DONE)

    # Rescheduling section. If landing bucket is empty, issue NEW_FILES_IMPORTED Asset, otherwise trigger dag

    @task
    def s3_list_remaining() -> list[str]:
        s3_hook = S3Hook(aws_conn_id=Conn.MINIO)
        return s3_hook.list_keys(bucket_name=Variable.get(VariableName.BUCKET_LANDING), max_items=1)

    _s3_list_remaining = s3_list_remaining()

    _s3_import >> import_sync >> _s3_list_remaining

    def choose_next_path(**kwargs):
        return 'import_incomplete' if kwargs['ti'].xcom_pull(task_ids='s3_list_remaining') else 'import_complete'

    branch = BranchPythonOperator(task_id='branch', python_callable=choose_next_path)
    import_incomplete = EmptyOperator(task_id='import_incomplete')
    import_complete = EmptyOperator(task_id='import_complete', outlets=[Asset(AssetName.IMPORT_COMPLETE)])
    trigger_again = TriggerDagRunOperator(task_id='trigger_again', trigger_dag_id=DagName.IMPORT_LANDING_FILES_S3)

    _s3_list_remaining >> branch >> [import_incomplete, import_complete]
    import_complete >> finish
    import_incomplete >> trigger_again >> finish


dag()
