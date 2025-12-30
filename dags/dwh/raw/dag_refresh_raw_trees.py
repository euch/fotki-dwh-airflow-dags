from dataclasses import dataclass
from datetime import timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, TaskGroup, DAG, Variable
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import DeltaTriggerTimetable

from config import *
from bulk_insert import BulkInsertOperator
from create_remote_tree_csv import CreateRemoteTreeCsvOperator
from smb_download import SmbDownloadOperator

schedule = AssetOrTimeSchedule(
    timetable=DeltaTriggerTimetable(timedelta(hours=1)),
    assets=(Asset(AssetName.NEW_FILES_IMPORTED)))
tags = {
    DagTag.DWH_RAW,
    DagTag.STORAGE_IO,
    DagTag.PG,
    DagTag.SMB,
    DagTag.SSH,
}


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
        tree_file_csv_remote_path=Variable.get(VariableName.STORAGE_PATH_ARCHIVE),
        tree_file_csv_local_path="/tmp/tree_archive.csv",
        pg_tree_table="raw.tree_archive"
    ),
    RefreshRawConf(
        group_id='collection',
        smb_conn_name=Conn.SMB_COLLECTION,
        tree_file_csv_remote_path=Variable.get(VariableName.STORAGE_PATH_COLLECTION),
        tree_file_csv_local_path="/tmp/tree_collection.csv",
        pg_tree_table="raw.tree_collection"
    ),
    RefreshRawConf(
        group_id='trash',
        smb_conn_name=Conn.SMB_TRASH,
        tree_file_csv_remote_path=Variable.get(VariableName.STORAGE_PATH_TRASH),
        tree_file_csv_local_path="/tmp/tree_trash.csv",
        pg_tree_table="raw.tree_trash"
    )
]

with DAG(dag_id=DagName.REFRESH_RAW_TREES, max_active_runs=1, default_args=dag_args_noretry, schedule=schedule,
         tags=tags) as dag:
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

refresh_raw >> EmptyOperator(task_id="finish", outlets=Asset(AssetName.RAW_TREES_UPDATED))
