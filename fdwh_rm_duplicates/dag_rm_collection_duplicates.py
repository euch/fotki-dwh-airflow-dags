from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.sdk import Variable, DAG

from fdwh_config import *


def _delete_selected_collection_duplicates():
    pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
    smb_hook_collection = SambaHook.get_hook(Conn.SMB_COLLECTION)
    records = pg_hook.get_records(
        sql='select abs_filename, "hash" from duplicates.collection_duplicates where "delete" is true;')
    for r in records:
        abs_filename, hash = r[0], r[1]
        rel_filename = abs_filename.removeprefix(Variable.get(VariableName.STORAGE_PATH_COLLECTION)).removeprefix("/")
        print(f"Removing {rel_filename}")
        smb_hook_collection.remove(rel_filename)
        pg_hook.run(sql=f'''delete from duplicates.collection_duplicates where "hash" = '{hash}';''')


tags = {
    DagTag.FDWH_DUPLICATES,
    DagTag.FDWH_STORAGE_IO,
    DagTag.PG,
    DagTag.SMB,
    DagTag.CLEANUP,
}
with DAG(dag_id=DagName.RM_COLLECTION_DUPLICATES, max_active_runs=1, default_args=dag_args_noretry, tags=tags):
    _ = PythonOperator(
        task_id='delete_selected_collection_duplicates',
        python_callable=_delete_selected_collection_duplicates)
