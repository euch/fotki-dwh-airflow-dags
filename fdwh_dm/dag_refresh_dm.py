from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, Asset

from fdwh_config import *


@dag(dag_display_name=DagName.REFRESH_DATAMARTS,
     schedule=(Asset(AssetName.CORE_METADATA_UPDATED) | Asset(AssetName.CORE_AI_DESCR_UPDATED)),
     default_args=dag_args_retry, max_active_runs=1)
def update_dm():
    SQLExecuteQueryOperator(
        task_id='dm_counts_insert',
        conn_id=Conn.POSTGRES,
        sql='sql/dm_counts_insert.sql'
    ) >> SQLExecuteQueryOperator(
        task_id='dm_file_types_insert',
        conn_id=Conn.POSTGRES,
        sql='sql/dm_file_types_insert.sql',
        do_xcom_push=False
    )


update_dm()
