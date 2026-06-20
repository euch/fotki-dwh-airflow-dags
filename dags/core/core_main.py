from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from config import *
from core import DagId

schedule = CronTriggerTimetable('10,40 * * * *', timezone='UTC')
tags = {
    DagTag.DWH_CORE,
    DagTag.PG,
}
with (DAG(dag_id=DagId.CORE_MAIN, max_active_runs=1, schedule=schedule, default_args=dag_args_noretry, tags=tags)):
    trigger_tree_update = TriggerDagRunOperator(task_id='trigger_tree_update',
                                                trigger_dag_id=DagId.CORE_TREE_UPDATE.value,
                                                reset_dag_run=True,
                                                wait_for_completion=True)

    trigger_metadata_update = TriggerDagRunOperator(task_id='trigger_metadata_update',
                                                    trigger_dag_id=DagId.CORE_METADATA_UPDATE.value,
                                                    reset_dag_run=True,
                                                    wait_for_completion=True)

    trigger_caption_update = TriggerDagRunOperator(task_id='trigger_caption_update',
                                                   trigger_dag_id=DagId.CORE_CAPTION_UPDATE.value,
                                                   reset_dag_run=True,
                                                   wait_for_completion=True)

    trigger_tree_update >> trigger_metadata_update >> trigger_caption_update
