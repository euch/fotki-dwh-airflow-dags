from datetime import timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, Asset
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import DeltaTriggerTimetable

from config import *

schedule = AssetOrTimeSchedule(
    timetable=DeltaTriggerTimetable(timedelta(hours=1)),
    assets=[Asset(AssetName.CORE_UPDATED)])


@dag(schedule=schedule, default_args=dag_args_noretry, max_active_runs=1)
def dm_update():
    SQLExecuteQueryOperator(
        task_id='dm_counts_insert',
        conn_id=Conn.POSTGRES,
        sql='sql/counts_insert.sql')

    SQLExecuteQueryOperator(
        task_id='dm_file_types_insert',
        conn_id=Conn.POSTGRES,
        sql='sql/files_and_types_insert.sql',
        do_xcom_push=False)

    SQLExecuteQueryOperator(
        task_id='dm_total_counts_insert',
        conn_id=Conn.POSTGRES,
        sql='sql/total_count_by_type_insert.sql',
        do_xcom_push=False)

    SQLExecuteQueryOperator(
        task_id='dm_preview_count_by_type_insert',
        conn_id=Conn.POSTGRES,
        sql='sql/preview_count_by_type_insert.sql',
        do_xcom_push=False)

    SQLExecuteQueryOperator(
        task_id='caption_count_by_type',
        conn_id=Conn.POSTGRES,
        sql='sql/caption_count_by_type_insert.sql',
        do_xcom_push=False)

    SQLExecuteQueryOperator(
        task_id='collection_duplicates_truncate_insert',
        conn_id=Conn.POSTGRES,
        sql='sql/collection_duplicates_truncate_insert.sql',
        do_xcom_push=False)


dm_update()
