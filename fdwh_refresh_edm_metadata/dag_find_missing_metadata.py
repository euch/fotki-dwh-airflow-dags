from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Asset, DAG

from fdwh_config import *

with DAG(
        dag_id=DagName.FIND_MISSING_METADATA,
        max_active_runs=1,
        schedule=Asset(AssetName.EDM_TREE_UPDATED)):
    SQLExecuteQueryOperator(
        task_id='find_missing_metadata_archive',
        conn_id=Conn.POSTGRES,
        sql='sql/find_missing_metadata_archive.sql',
        outlets=Asset(AssetName.MISSING_METADATA_ARCHIVE))

    SQLExecuteQueryOperator(
        task_id='find_missing_metadata_collection',
        conn_id=Conn.POSTGRES,
        sql='sql/find_missing_metadata_collection.sql',
        outlets=Asset(AssetName.MISSING_METADATA_COLLECTION)),

    SQLExecuteQueryOperator(
        task_id='find_missing_metadata_trash',
        conn_id=Conn.POSTGRES,
        sql='sql/find_missing_metadata_trash.sql',
        outlets=Asset(AssetName.MISSING_METADATA_TRASH))
