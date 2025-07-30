from datetime import timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, Variable, dag
from airflow.timetables.trigger import DeltaTriggerTimetable
from airflow.utils.trigger_rule import TriggerRule

from fdwh_config import VariableName, AssetName, dag_args_noretry, DagName
from fdwh_op_check_helper_available import CheckHelperAvailableOperator


@dag(dag_id=DagName.PING_SERVICES, max_active_runs=1, default_args=dag_args_noretry, schedule=DeltaTriggerTimetable(timedelta(hours=1)))
def dag():

    finish = EmptyOperator(task_id='finish', trigger_rule=TriggerRule.ALL_DONE)

    CheckHelperAvailableOperator(
        task_id="check_metadata",
        url=Variable.get(VariableName.METADATA_ENDPOINT)) >> finish

    CheckHelperAvailableOperator(
        task_id="check_exif_ts",
        url=Variable.get(VariableName.EXIF_TS_ENDPOINT)) >> finish

    CheckHelperAvailableOperator(
        task_id="check_ai_desc",
        url=Variable.get(VariableName.AI_DESCR_ENDPOINT)) >> finish


dag()
