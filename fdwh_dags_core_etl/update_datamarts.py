from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, Asset

from fdwh_config import *


@dag(dag_display_name=DagName.UPDATE_DATAMARTS, schedule=[
    Asset(AssetName.ADD_METADATA_ARCHIVE),
    Asset(AssetName.ADD_METADATA_COLLECTION),
    Asset(AssetName.ADD_METADATA_TRASH),
    Asset(AssetName.ADD_AI_DESCR_COLLECTION)
], default_args=dag_default_args)
def update_dm():
    SQLExecuteQueryOperator(
        task_id='dm_counts_insert',
        conn_id=Conn.POSTGRES,
        sql='sql/dm/dm_counts_insert.sql'
    ) >> SQLExecuteQueryOperator(
        task_id='dm_file_types_insert',
        conn_id=Conn.POSTGRES,
        sql='sql/dm/dm_file_types_insert.sql',
        do_xcom_push=False
    )


update_dm()
