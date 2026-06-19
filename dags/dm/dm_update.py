from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, Asset

from config import *

schedule = [
    Asset(AssetName.CORE_TREE_UPDATED) | Asset(AssetName.CORE_METADATA_UPDATED) | Asset(AssetName.CORE_CAPTION_UPDATED)]
tags = {
    DagTag.DWH_MARTS,
    DagTag.PG,
}


@dag(schedule=schedule, tags=tags, default_args=dag_args_retry, max_active_runs=1)
def dm_update():
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


dm_update()
