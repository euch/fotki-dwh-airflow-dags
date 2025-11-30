from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, BranchSQLOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, DAG

from fdwh_config import *

schedule = [Asset(AssetName.RAW_TREES_UPDATED)]
tags = {
    DagTag.FDWH_CORE,
    DagTag.PG,
}
with DAG(dag_id=DagName.REFRESH_CORE_TREE, max_active_runs=1, schedule=schedule, default_args=dag_args_noretry,
         tags=tags):

    SQLExecuteQueryOperator(
        task_id='tree_delete_old',
        conn_id=Conn.POSTGRES,
        sql='sql/tree_delete_old.sql'
    ) >> SQLExecuteQueryOperator(
        task_id='tree_insert_new',
        conn_id=Conn.POSTGRES,
        sql='sql/tree_insert_new.sql'
    ) >> EmptyOperator(task_id="finish", outlets=Asset(AssetName.CORE_TREE_UPDATED))
