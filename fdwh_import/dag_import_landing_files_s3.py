from datetime import timedelta

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.sdk import Asset, task, dag, Variable
from airflow.timetables.trigger import DeltaTriggerTimetable

from fdwh_config import *
from fdwh_import.dto.import_item import GoodImportItem, ImportItem
from fdwh_import.utils.create_import_item import create_import_item
from fdwh_import.utils.move_s3_import_item import move_s3_import_item
from fdwh_op_check_helper_available import CheckHelperAvailableOperator


@dag(
    dag_id=DagName.IMPORT_LANDING_FILES_S3,
    max_active_runs=1,
    default_args=dag_default_args,
    schedule=DeltaTriggerTimetable(timedelta(minutes=1)),
)
def dag():
    wait_for_any_s3_file = S3KeySensor(
        task_id='wait_for_any_s3_file',
        bucket_key='*',  # Use '*' to match any file within the prefix
        bucket_name=Variable.get(VariableName.BUCKET_LANDING),  # Replace with your actual bucket name
        wildcard_match=True,  # Enable wildcard matching
        poke_interval=60,  # Check every 60 seconds (adjust as needed)
        timeout=3600,  # Timeout after 1 hour (adjust as needed)
        aws_conn_id=Conn.MINIO,  # Replace with your AWS connection ID if not default
        mode='poke'  # Use 'poke' for regular polling.  'reschedule' is an alternative.
    )

    assert_exif_helper_available = CheckHelperAvailableOperator(
        task_id="assert_exif_helper_available",
        url=Variable.get(VariableName.EXIF_TS_ENDPOINT),
        outlets=[Asset(AssetName.EXIF_TS_HELPER_AVAIL)])

    @task(outlets=[Asset(AssetName.NEW_FILES_IMPORTED)])
    def import_landing_files() -> list[str]:

        s3 = S3Hook(aws_conn_id=Conn.MINIO).get_client_type('s3')
        smb_hook_storage = SambaHook.get_hook(Conn.SMB_COLLECTION)
        pg_hook = PostgresHook.get_hook(Conn.POSTGRES)

        landing_bucket = Variable.get(VariableName.BUCKET_LANDING)
        exif_ts_endpoint = Variable.get(VariableName.EXIF_TS_ENDPOINT)
        unrecognized_bucket = Variable.get(VariableName.BUCKET_REJECTED_UNSUPPORTED)
        duplicate_bucket = Variable.get(VariableName.BUCKET_REJECTED_DUPLICATES)

        imported_storage_paths: list[str] = []

        for page in s3.get_paginator('list_objects_v2').paginate(Bucket=landing_bucket):
            if 'Contents' in page:
                for obj in page['Contents']:
                    import_item: ImportItem = create_import_item(s3=s3, pg_hook=pg_hook, landing_bucket_key=obj['Key'],
                                                     landing_bucket=landing_bucket, exif_ts_endpoint=exif_ts_endpoint,
                                                     unrecognized_bucket=unrecognized_bucket,
                                                     duplicate_bucket=duplicate_bucket)

                    move_s3_import_item(s3=s3, smb_hook_storage=smb_hook_storage, import_item=import_item)

                    if isinstance(import_item, GoodImportItem):
                        imported_storage_paths.append(import_item.storage_path)

        return imported_storage_paths

    wait_for_any_s3_file >> assert_exif_helper_available >> import_landing_files()


dag()
