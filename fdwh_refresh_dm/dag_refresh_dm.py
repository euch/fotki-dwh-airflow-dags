from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, Asset

from fdwh_config import *

ma = Asset(AssetName.ADD_METADATA_ARCHIVE)
mc = Asset(AssetName.ADD_METADATA_COLLECTION)
mt = Asset(AssetName.ADD_METADATA_TRASH)
aidc = Asset(AssetName.ADD_AI_DESCR_COLLECTION)


@dag(dag_display_name=DagName.UPDATE_DATAMARTS,
     schedule=((ma & mc & mt) | (aidc & ma & mc & mt)),
     default_args=dag_default_args)
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
