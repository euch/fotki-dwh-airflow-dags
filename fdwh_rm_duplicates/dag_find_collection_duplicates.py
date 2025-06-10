from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from fdwh_config import *

with DAG(dag_id=DagName.FIND_COLLECTION_DUPLICATES, max_active_runs=1, schedule=SCHEDULE_MANUAL,
         default_args=dag_default_args):
    dm_collection_duplicates_truncate = SQLExecuteQueryOperator(
        task_id='dm_collection_duplicates_truncate',
        conn_id=Conn.POSTGRES,
        sql='truncate table duplicates.collection_duplicates;')
    dm_collection_duplicates_insert = SQLExecuteQueryOperator(
        task_id='dm_collection_duplicates_insert',
        conn_id=Conn.POSTGRES,
        sql='sql/dm_collection_duplicates_insert.sql')
    dm_collection_duplicates_truncate >> dm_collection_duplicates_insert
