from datetime import timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import DeltaTriggerTimetable

from assets import CORE_METADATA_UPDATED, CORE_CAPTION_UPDATED, CORE_TREE_UPDATED
from connections import POSTGRES

schedule = AssetOrTimeSchedule(
    timetable=DeltaTriggerTimetable(timedelta(hours=1)),
    assets=[CORE_TREE_UPDATED, CORE_METADATA_UPDATED, CORE_CAPTION_UPDATED])


@dag(schedule=schedule, default_args={'retries': 0}, max_active_runs=1)
def dm_update():
    SQLExecuteQueryOperator(
        task_id='dm_counts_insert',
        conn_id=POSTGRES,
        sql='sql/counts_insert.sql')

    SQLExecuteQueryOperator(
        task_id='dm_file_types_insert',
        conn_id=POSTGRES,
        sql='sql/files_and_types_insert.sql',
        do_xcom_push=False)

    SQLExecuteQueryOperator(
        task_id='dm_total_counts_insert',
        conn_id=POSTGRES,
        sql='sql/total_count_by_type_insert.sql',
        do_xcom_push=False)

    SQLExecuteQueryOperator(
        task_id='dm_preview_count_by_type_insert',
        conn_id=POSTGRES,
        sql='sql/preview_count_by_type_insert.sql',
        do_xcom_push=False)

    SQLExecuteQueryOperator(
        task_id='caption_count_by_type',
        conn_id=POSTGRES,
        sql='sql/caption_count_by_type_insert.sql',
        do_xcom_push=False)

    SQLExecuteQueryOperator(
        task_id='collection_duplicates_truncate_insert',
        conn_id=POSTGRES,
        sql='sql/collection_duplicates_truncate_insert.sql',
        do_xcom_push=False)


dm_update()
