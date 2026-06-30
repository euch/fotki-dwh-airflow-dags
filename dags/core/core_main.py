from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from config import *
from core import DagId

schedule = CronTriggerTimetable('*/5 * * * *', timezone='UTC')

with (DAG(dag_id=DagId.CORE_MAIN, max_active_runs=1, schedule=schedule, default_args=dag_args_noretry)):
    tree_diff_sensor = SqlSensor(
        task_id='tree_diff_sensor',
        conn_id=Conn.POSTGRES,
        sql='sql/tree_select_diff.sql',
        soft_fail=True,
        mode='poke',
        poke_interval=60 * 5,  # Check every 5 minutes
        timeout=60 * 60 * 24,  # Timeout after 24 hour
    )

    trigger_tree_update = TriggerDagRunOperator(task_id='trigger_tree_update',
                                                trigger_dag_id=DagId.CORE_TREE_UPDATE.value,
                                                skip_when_already_exists = True,
                                                wait_for_completion=True)

    trigger_metadata_update = TriggerDagRunOperator(task_id='trigger_metadata_update',
                                                    trigger_dag_id=DagId.CORE_METADATA_UPDATE.value,
                                                    skip_when_already_exists = True,
                                                    wait_for_completion=True)

    trigger_caption_update = TriggerDagRunOperator(task_id='trigger_caption_update',
                                                   trigger_dag_id=DagId.CORE_CAPTION_UPDATE.value,
                                                   skip_when_already_exists = True,
                                                   wait_for_completion=False)

    tree_diff_sensor >> trigger_tree_update >> trigger_metadata_update >> trigger_caption_update
