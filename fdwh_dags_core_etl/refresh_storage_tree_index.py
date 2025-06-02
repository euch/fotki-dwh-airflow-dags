from dataclasses import dataclass

from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import TaskGroup, DAG, Asset

from fdwh_config import *
from fdwh_operators.bulk_insert_operator import BulkInsertOperator
from fdwh_operators.create_remote_tree_csv_operator import CreateRemoteTreeCsvOperator
from fdwh_operators.smb_download_operator import SmbDownloadOperator


@dataclass
class RefreshRawConf:
    group_id: str
    smb_conn_name: str
    tree_file_csv_remote_path: str
    tree_file_csv_local_path: str
    pg_tree_table: str


REFRESH_RAW_CONFS: list[RefreshRawConf] = [
    RefreshRawConf(
        group_id='archive',
        smb_conn_name=Conn.SMB_ARCHIVE,
        tree_file_csv_remote_path=Variable.get(VariableName.RP_ARCHIVE),
        tree_file_csv_local_path="/tmp/tree_archive.csv",
        pg_tree_table="raw.tree_archive"
    ),
    RefreshRawConf(
        group_id='collection',
        smb_conn_name=Conn.SMB_COLLECTION,
        tree_file_csv_remote_path=Variable.get(VariableName.RP_COLLECTION),
        tree_file_csv_local_path="/tmp/tree_collection.csv",
        pg_tree_table="raw.tree_collection"
    ),
    RefreshRawConf(
        group_id='trash',
        smb_conn_name=Conn.SMB_TRASH,
        tree_file_csv_remote_path=Variable.get(VariableName.RP_TRASH),
        tree_file_csv_local_path="/tmp/tree_trash.csv",
        pg_tree_table="raw.tree_trash"
    )
]

with DAG(dag_id=DagName.REFRESH_STORAGE_TREE_INDEX, max_active_runs=1, schedule=SCHEDULE_DAILY):
    with TaskGroup(group_id="refresh_raw") as refresh_raw:
        for rrc in REFRESH_RAW_CONFS:
            with TaskGroup(group_id=rrc.group_id):
                CreateRemoteTreeCsvOperator(
                    task_id="create_remote_tree_file",
                    ssh_conn_id=Conn.SSH_STORAGE,
                    location_rp=rrc.tree_file_csv_remote_path,
                    remote_csv_name='tree.csv'
                ) >> SmbDownloadOperator(
                    task_id="download_tree_file_to_local_path",
                    smb_conn_name=rrc.smb_conn_name,
                    remote_path='tree.csv',
                    local_path=rrc.tree_file_csv_local_path
                ) >> BulkInsertOperator(
                    task_id="bulk_insert_tree_file_content",
                    pg_conn_name=Conn.POSTGRES,
                    local_csv_path=rrc.tree_file_csv_local_path,
                    table_name=rrc.pg_tree_table
                )

    with TaskGroup(group_id="refresh_tree") as refresh_tree:
        refresh_raw >> SQLExecuteQueryOperator(task_id='tree_delete_old', conn_id=Conn.POSTGRES,
                                               sql='sql/edm/edm_tree_delete_old.sql')
        refresh_raw >> SQLExecuteQueryOperator(task_id='tree_insert_new', conn_id=Conn.POSTGRES,
                                               sql='sql/edm/edm_tree_insert_new.sql')

    refresh_raw >> refresh_tree

    find_missing_metadata_archive = SQLExecuteQueryOperator(
        task_id='find_missing_metadata_archive',
        conn_id=Conn.POSTGRES,
        sql='sql/edm/find_missing_metadata_archive.sql',
        outlets=Asset(AssetName.MISSING_METADATA_ARCHIVE))

    find_missing_metadata_archive_collection = SQLExecuteQueryOperator(
        task_id='find_missing_metadata_collection',
        conn_id=Conn.POSTGRES,
        sql='sql/edm/find_missing_metadata_collection.sql',
        outlets=Asset(AssetName.MISSING_METADATA_COLLECTION))

    find_missing_metadata_trash = SQLExecuteQueryOperator(
        task_id='find_missing_metadata_trash',
        conn_id=Conn.POSTGRES,
        sql='sql/edm/find_missing_metadata_trash.sql',
        outlets=Asset(AssetName.MISSING_METADATA_TRASH))

    find_missing_ai_descr_collection = SQLExecuteQueryOperator(
        task_id='find_missing_ai_descr_collection',
        conn_id=Conn.POSTGRES,
        sql='sql/edm/find_missing_ai_descr_collection.sql',
        outlets=Asset(AssetName.MISSING_AI_DESCR_COLLECTION))

    refresh_tree >> [
        find_missing_metadata_archive,
        find_missing_metadata_archive_collection,
        find_missing_metadata_trash,
        find_missing_ai_descr_collection,
    ]

#
# with TaskGroup(group_id='dm') as dm:
#     SQLExecuteQueryOperator(
#         task_id='insert_counts_row',
#         conn_id=Conn.POSTGRES,
#         sql='sql/dm/dm_counts_insert.sql'
#     )
#     SQLExecuteQueryOperator(
#         task_id='insert_file_types_rows',
#         conn_id=Conn.POSTGRES,
#         sql='sql/dm/dm_file_types_insert.sql'
#     )
#
# raw >> edm >> dm
