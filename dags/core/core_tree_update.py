from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.sdk import dag
from airflow.timetables.trigger import CronTriggerTimetable

from assets import CORE_TREE_UPDATED, CORE_TREE_DIFF_FOUND
from connections import POSTGRES


@dag(max_active_runs=1, schedule=CronTriggerTimetable('*/5 * * * *', timezone='UTC'), default_args={'retries': 0})
def core_tree_diff_wait():
    SqlSensor(
        task_id='tree_diff_sensor',
        conn_id=POSTGRES,
        mode='poke',
        outlets=[CORE_TREE_DIFF_FOUND],
        poke_interval=60 * 5,  # Check every 5 minutes
        soft_fail=True,
        sql='sql/tree_select_diff.sql',
        timeout=60 * 60 * 24)  # Timeout after 24 hour


core_tree_diff_wait()


@dag(max_active_runs=1, schedule=[CORE_TREE_DIFF_FOUND], default_args={'retries': 0})
def core_tree_update():
    tree_delete_old = SQLExecuteQueryOperator(
        task_id='tree_delete_old',
        conn_id=POSTGRES,
        sql='sql/tree_delete_old.sql')

    tree_insert_new = SQLExecuteQueryOperator(
        task_id='tree_insert_new',
        conn_id=POSTGRES,
        sql='sql/tree_insert_new.sql',
        outlets=[CORE_TREE_UPDATED])

    tree_delete_old >> tree_insert_new


core_tree_update()
