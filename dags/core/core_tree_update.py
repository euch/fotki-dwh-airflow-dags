from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, Variable, dag

from config import *
from core import DagId


@dag(dag_id=DagId.CORE_TREE_UPDATE, max_active_runs=1, default_args=dag_args_noretry)
def dag():
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
    ) >> EmptyOperator(task_id="finish", outlets=Asset(AssetName.CORE_UPDATED))


dag()
