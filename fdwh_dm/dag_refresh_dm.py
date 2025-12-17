from datetime import timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, Asset
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import DeltaTriggerTimetable

from fdwh_config import *

assets = (Asset(AssetName.CORE_TREE_UPDATED) | Asset(AssetName.CORE_METADATA_UPDATED) | Asset(
    AssetName.CORE_CAPTION_UPDATED))
schedule = AssetOrTimeSchedule(
    timetable=DeltaTriggerTimetable(timedelta(hours=1)),
    assets=assets)
tags = {
    DagTag.FDWH_MARTS,
    DagTag.PG,
}


@dag(dag_display_name=DagName.REFRESH_DATAMARTS, schedule=schedule, tags=tags, default_args=dag_args_retry,
     max_active_runs=1)
def update_dm():
    SQLExecuteQueryOperator(
        task_id='dm_counts_insert',
        conn_id=Conn.POSTGRES,
        sql='sql/dm_counts_insert.sql')

    SQLExecuteQueryOperator(
        task_id='dm_file_types_insert',
        conn_id=Conn.POSTGRES,
        sql='sql/dm_file_types_insert.sql',
        do_xcom_push=False)

    SQLExecuteQueryOperator(
        task_id='dm_total_counts_insert',
        conn_id=Conn.POSTGRES,
        sql='sql/dm_total_count_by_type_insert.sql',
        do_xcom_push=False)

    SQLExecuteQueryOperator(
        task_id='dm_preview_count_by_type_insert',
        conn_id=Conn.POSTGRES,
        sql='sql/dm_preview_count_by_type_insert.sql',
        do_xcom_push=False)

    SQLExecuteQueryOperator(
        task_id='caption_count_by_type',
        conn_id=Conn.POSTGRES,
        sql='sql/dm_caption_count_by_type.sql',
        do_xcom_push=False)


update_dm()
