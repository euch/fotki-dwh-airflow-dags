from airflow import Asset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag

from config import *
from core import DagId


@dag(dag_id=DagId.CORE_TREE_UPDATE, max_active_runs=1, default_args=dag_args_noretry)
def dag():
    SQLExecuteQueryOperator(
        task_id='tree_delete_old',
        conn_id=Conn.POSTGRES,
        sql='sql/tree_delete_old.sql'
    ) >> SQLExecuteQueryOperator(
        task_id='tree_insert_new',
        conn_id=Conn.POSTGRES,
        sql='sql/tree_insert_new.sql'
    ) >> EmptyOperator(task_id="finish", outlets=Asset(AssetName.CORE_UPDATED))


dag()
