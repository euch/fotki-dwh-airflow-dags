from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.samba.hooks.samba import SambaHook

from fdwh_config import *


def delete_selected_collection_duplicates():
    pg_hook = PostgresHook.get_hook(CONN_POSTGRES)
    smb_hook_collection = SambaHook.get_hook(CONN_SMB_COLLECTION)
    records = pg_hook.get_records(
        sql='select abs_filename, "hash" from duplicates.collection_duplicates where "delete" is true;')
    for r in records:
        abs_filename, hash = r[0], r[1]
        rel_filename = abs_filename.removeprefix(Variable.get(VAR_RP_COLLECTION)).removeprefix("/")
        print(f"Removing {rel_filename}")
        smb_hook_collection.remove(rel_filename)
        pg_hook.run(sql=f'''delete from duplicates.collection_duplicates where "hash" = '{hash}';''')


with DAG(dag_id=DAG_NAME_DWH_RM_COLLECTION_DUPLICATES, max_active_runs=1, schedule=SCHEDULE_MANUAL):
    delete_selected_collection_duplicates = PythonOperator(
        task_id='delete_selected_collection_duplicates',
        python_callable=delete_selected_collection_duplicates)
    trigger_dwh_refresh = TriggerDagRunOperator(
        task_id="trigger_" + DAG_NAME_DWH_REFRESH,
        trigger_dag_id=DAG_NAME_DWH_REFRESH,
        wait_for_completion=False),
    delete_selected_collection_duplicates >> trigger_dwh_refresh
