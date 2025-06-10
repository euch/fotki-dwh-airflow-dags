from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Asset, DAG

from fdwh_config import *

with DAG(
        dag_id=DagName.FIND_MISSING_AI_DESCR,
        max_active_runs=1,
        schedule=[Asset(AssetName.EDM_TREE_UPDATED)]
):
    SQLExecuteQueryOperator(
        task_id='find_missing_ai_descr_collection',
        conn_id=Conn.POSTGRES,
        sql='sql/find_missing_ai_descr_collection.sql',
        outlets=Asset(AssetName.MISSING_AI_DESCR_COLLECTION))
