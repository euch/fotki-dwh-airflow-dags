from airflow import DAG
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

from fdwh_config import *
from fdwh_operators.add_ai_descr_operator import AddAiDescrVitGpt2Operator
from fdwh_operators.add_metadata_operator import AddMetadataOperator
from fdwh_operators.bulk_insert_operator import BulkInsertOperator
from fdwh_operators.create_remote_tree_csv_operator import CreateRemoteTreeCsvOperator
from fdwh_operators.smb_download_operator import SmbDownloadOperator

with (DAG(dag_id=DAG_NAME_DWH_REFRESH, max_active_runs=1, schedule=SCHEDULE_DAILY) as dag):
    def refresh_raw(smb_conn_name: str, tree_file_csv_rp: str, tree_file_csv_lp: str, pg_tree_table: str):
        CreateRemoteTreeCsvOperator(
            task_id="create",
            ssh_conn_id=CONN_SSH_STORAGE,
            location_rp=tree_file_csv_rp,
            remote_csv_name='tree.csv'
        ) >> SmbDownloadOperator(
            task_id="dl",
            smb_conn_name=smb_conn_name,
            remote_path='tree.csv',
            local_path=tree_file_csv_lp
        ) >> BulkInsertOperator(
            task_id="bulk_insert",
            pg_conn_name=CONN_POSTGRES,
            local_csv_path=tree_file_csv_lp,
            table_name=pg_tree_table)


    with TaskGroup(group_id='raw') as raw:
        with TaskGroup(group_id='refresh_archive'):
            refresh_raw(CONN_SMB_ARCHIVE,
                        tree_file_csv_rp=Variable.get(VAR_RP_ARCHIVE),
                        tree_file_csv_lp="/tmp/tree_archive.csv",
                        pg_tree_table="raw.tree_archive")

        with TaskGroup(group_id='refresh_collection'):
            refresh_raw(CONN_SMB_COLLECTION,
                        tree_file_csv_rp=Variable.get(VAR_RP_COLLECTION),
                        tree_file_csv_lp="/tmp/tree_collection.csv",
                        pg_tree_table="raw.tree_collection")

        with TaskGroup(group_id='refresh_trash'):
            refresh_raw(CONN_SMB_TRASH,
                        tree_file_csv_rp=Variable.get(VAR_RP_TRASH),
                        tree_file_csv_lp="/tmp/tree_trash.csv",
                        pg_tree_table="raw.tree_trash")

    with TaskGroup(group_id='edm') as edm:
        tree_delete_old = SQLExecuteQueryOperator(task_id='tree_delete_old', conn_id=CONN_POSTGRES,
                                                  sql='sql/edm/edm_tree_delete_old.sql')
        tree_insert_new = SQLExecuteQueryOperator(task_id='tree_insert_new', conn_id=CONN_POSTGRES,
                                                  sql='sql/edm/edm_tree_insert_new.sql')

        with TaskGroup(group_id='add_missing_metadata') as add_missing_metadata:
            def add_new(find_missing_metadata_sql: str, xcom_key: str, smb_conn_name: str, rp: str):
                SQLExecuteQueryOperator(
                    task_id='find_missing',
                    conn_id=CONN_POSTGRES,
                    sql=find_missing_metadata_sql
                ) >> AddMetadataOperator(
                    task_id='add_missing',
                    xcom_key=xcom_key,
                    pg_conn_name=CONN_POSTGRES,
                    smb_conn_name=smb_conn_name,
                    metadata_endpoint=Variable.get(VAR_METADATA_ENDPOINT),
                    filename_converter_func=lambda abs_filename: abs_filename.replace(rp, ''))


            with TaskGroup(group_id='archive') as add_metadata_archive:
                add_new(find_missing_metadata_sql='sql/edm/find_missing_metadata_archive.sql',
                        xcom_key='edm.add_missing_metadata.archive.find_missing',
                        smb_conn_name=CONN_SMB_ARCHIVE,
                        rp=Variable.get(VAR_RP_ARCHIVE))

            with TaskGroup(group_id='collection') as add_metadata_collection:
                add_new(find_missing_metadata_sql='sql/edm/find_missing_metadata_collection.sql',
                        xcom_key='edm.add_missing_metadata.collection.find_missing',
                        smb_conn_name=CONN_SMB_COLLECTION,
                        rp=Variable.get(VAR_RP_COLLECTION))

            with TaskGroup(group_id='trash') as add_metadata_trash:
                add_new(find_missing_metadata_sql='sql/edm/find_missing_metadata_trash.sql',
                        xcom_key='edm.add_missing_metadata.trash.find_missing',
                        smb_conn_name=CONN_SMB_TRASH,
                        rp=Variable.get(VAR_RP_TRASH))

        with TaskGroup(group_id='add_missing_ai_descr') as add_missing_ai_descr:

            with TaskGroup(group_id='collection') as add_ai_descr_collection:
                SQLExecuteQueryOperator(
                    task_id='find_missing',
                    conn_id=CONN_POSTGRES,
                    sql='sql/edm/find_missing_ai_descr_collection.sql'
                ) >> AddAiDescrVitGpt2Operator(
                    task_id='add_missing',
                    xcom_key='edm.add_missing_ai_descr.collection.find_missing',
                    pg_conn_name=CONN_POSTGRES,
                    ai_descr_endpoint=Variable.get(VAR_AI_DESCR_ENDPOINT))

        tree_delete_old >> tree_insert_new >> add_missing_metadata >> add_missing_ai_descr

    with TaskGroup(group_id='dm') as dm:
        SQLExecuteQueryOperator(
            task_id='insert_counts_row',
            conn_id=CONN_POSTGRES,
            sql='sql/dm/dm_counts_insert.sql')
        SQLExecuteQueryOperator(
            task_id='insert_file_types_rows',
            conn_id=CONN_POSTGRES,
            sql='sql/dm/dm_file_types_insert.sql')

    raw >> edm >> dm
