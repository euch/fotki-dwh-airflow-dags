from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, DAG

from fdwh_config import *

with DAG(dag_id=DagName.REFRESH_EDM_TREE,
         max_active_runs=1,
         schedule=[Asset(AssetName.RAW_TREES_UPDATED)],
         default_args=dag_default_args,
         ):
    SQLExecuteQueryOperator(
        task_id='tree_delete_old',
        conn_id=Conn.POSTGRES,
        sql='sql//edm_tree_delete_old.sql'
    ) >> SQLExecuteQueryOperator(
        task_id='tree_insert_new',
        conn_id=Conn.POSTGRES,
        sql='sql//edm_tree_insert_new.sql'
    ) >> EmptyOperator(task_id="finish", outlets=Asset(AssetName.EDM_TREE_UPDATED))
