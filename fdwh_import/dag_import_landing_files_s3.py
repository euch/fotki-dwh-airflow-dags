from datetime import timedelta

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Asset, dag, Variable, task
from airflow.timetables.trigger import DeltaTriggerTimetable

from fdwh_config import *
from fdwh_import.dto.import_item import GoodImportItem, ImportItem
from fdwh_import.utils.create_import_item import create_import_item
from fdwh_import.utils.move_s3_import_item import move_s3_import_item
from fdwh_op_check_helper_available import CheckHelperAvailableOperator

schedule = DeltaTriggerTimetable(timedelta(minutes=1))
tags = {
    DagTag.FDWH_HELPERS,
    DagTag.FDWH_STORAGE_IO,
    DagTag.PG,
    DagTag.S3,
    DagTag.SMB,
}


@dag(
    dag_id=DagName.IMPORT_LANDING_FILES_S3, max_active_runs=1, default_args=dag_args_noretry, schedule=schedule,
    tags=tags)
def dag():
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

    assert_exif_helper_available = CheckHelperAvailableOperator(
        task_id="assert_exif_helper_available",
        url=Variable.get(VariableName.EXIF_TS_ENDPOINT))

    @task()
    def import_landing_files() -> list[str]:

        s3 = S3Hook(aws_conn_id=Conn.MINIO).get_client_type('s3')
        smb_hook_storage = SambaHook.get_hook(Conn.SMB_COLLECTION)
        pg_hook = PostgresHook.get_hook(Conn.POSTGRES)

        import_max_file_size = Variable.get(VariableName.IMPORT_MAX_FILE_SIZE)
        landing_bucket = Variable.get(VariableName.BUCKET_LANDING)
        exif_ts_endpoint = Variable.get(VariableName.EXIF_TS_ENDPOINT)
        unsupported_bucket = Variable.get(VariableName.BUCKET_REJECTED_UNSUPPORTED)
        duplicate_bucket = Variable.get(VariableName.BUCKET_REJECTED_DUPLICATES)

        imported_storage_paths: list[str] = []

        for page in s3.get_paginator('list_objects_v2').paginate(Bucket=landing_bucket):
            if 'Contents' in page:
                for obj in page['Contents']:
                    import_item: ImportItem = create_import_item(s3=s3, pg_hook=pg_hook, landing_bucket_key=obj['Key'],
                                                                 landing_bucket=landing_bucket,
                                                                 exif_ts_endpoint=exif_ts_endpoint,
                                                                 unsupported_bucket=unsupported_bucket,
                                                                 duplicate_bucket=duplicate_bucket)

                    move_s3_import_item(s3=s3, smb_hook_storage=smb_hook_storage, import_item=import_item)

                    if isinstance(import_item, GoodImportItem):
                        imported_storage_paths.append(import_item.storage_path)

        return imported_storage_paths

    list_remaining_s3_files = S3ListOperator(
        task_id='list_remaining_s3_files',
        bucket=Variable.get(VariableName.BUCKET_LANDING),
        aws_conn_id=Conn.MINIO
    )

    def choose_next_path(**kwargs):
        return 'reschedule' if kwargs['ti'].xcom_pull(task_ids='list_remaining_s3_files') else 'finish'

    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=choose_next_path,
    )

    reschedule = TriggerDagRunOperator(
        task_id='reschedule',
        trigger_dag_id=DagName.IMPORT_LANDING_FILES_S3
    )

    finish = EmptyOperator(
        task_id='finish',
        outlets=[Asset(AssetName.NEW_FILES_IMPORTED)]
    )

    wait_for_any_s3_file >> assert_exif_helper_available >> import_landing_files() >> list_remaining_s3_files
    list_remaining_s3_files >> branch >> [reschedule, finish]


dag()
