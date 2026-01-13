from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, DAG, Variable

from config import *

schedule = [Asset(AssetName.RAW_TREES_UPDATED)]
tags = {
    DagTag.DWH_CORE,
    DagTag.PG,
}
with (DAG(dag_id=DagName.REFRESH_CORE_TREE, max_active_runs=1, schedule=schedule, default_args=dag_args_noretry,
          tags=tags)):
    SqlSensor(
        task_id='check_raw_differs',
        conn_id=Conn.POSTGRES,
        sql='sql/tree_select_diff.sql',
        soft_fail=True,
        mode='poke',
        poke_interval=60,
        timeout=5
    ) >> SQLExecuteQueryOperator(
        task_id='tree_delete_old',
        conn_id=Conn.POSTGRES,
        sql='sql/tree_delete_old.sql'
    ) >> SQLExecuteQueryOperator(
        task_id='tree_insert_new',
        conn_id=Conn.POSTGRES,
        sql='sql/tree_insert_new.sql'
    ) >> SQLExecuteQueryOperator(
        task_id='tree_rel_path_insert',
        conn_id=Conn.POSTGRES,
        sql='sql/tree_rel_path_insert.sql',
        parameters={
            'pattern_archive': '^' + Variable.get(VariableName.STORAGE_PATH_ARCHIVE),
            'pattern_collection': '^' + Variable.get(VariableName.STORAGE_PATH_COLLECTION),
            'pattern_trash': '^' + Variable.get(VariableName.STORAGE_PATH_TRASH)
        }
    ) >> EmptyOperator(task_id="finish", outlets=Asset(AssetName.CORE_TREE_UPDATED))
