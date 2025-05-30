import errno
import glob
import hashlib
import os
import shutil
from contextlib import contextmanager
from dataclasses import dataclass

import requests
import telebot
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.samba.hooks.samba import SambaHook
from dataclasses_json import dataclass_json

from fdwh_config import *

TG_USER_IDS = list(map(int, Variable.get(VariableName.TG_USER_IDS).split(',')))

PROCESS_EACH_LANDING_FILE_TASK_ID = 'process_each_landing_file'


@contextmanager
def ignorednr(exception, *errornrs):
    try:
        yield
    except exception as e:
        if e.errno not in errornrs:
            raise
        pass


@dataclass_json
@dataclass
class ProcessedFile:
    landing_path: str
    new_path: str | None


def process_each_landing_file():
    return process_files_recursively(Variable.get(VariableName.LP_LANDING))


def process_files_recursively(root_dir: str):
    pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
    smb_hook_collection = SambaHook.get_hook(Conn.SMB_COLLECTION)
    rejected_files, imported_images, companion_files = [], [], []
    for local_filepath in glob.iglob(root_dir + '/**', recursive=True):
        if os.path.exists(local_filepath) and os.path.isfile(local_filepath):
            process_file(imported_images, rejected_files, companion_files,
                         local_filepath, pg_hook, smb_hook_collection)
    return {
        "rejected_files": ProcessedFile.schema().dumps(rejected_files, many=True),
        "imported_images": ProcessedFile.schema().dumps(imported_images, many=True),
        "companion_files": ProcessedFile.schema().dumps(companion_files, many=True),
        "count_rejected_files": len(rejected_files),
        "count_imported_images": len(imported_images),
        "count_companion_files": len(companion_files),
    }


def process_file(imported_images, rejected_files, companion_files,
                 local_filepath, pg_hook, smb_hook_collection):
    print(f'[Process-File] looking for exif of the next local file {local_filepath}')
    exif_ts_endpoint = Variable.get(VariableName.EXIF_TS_ENDPOINT)
    with open(local_filepath, mode='rb') as file:
        files = {'file': file}
        response = requests.post(exif_ts_endpoint, files=files, data={'format': ImportSettings.TIMESTAMP_FMT})
        print(f'Exif-Timestamp endpoint responded with code {response.status_code}')
        assert response.status_code == 200
        metadata = response.json()
        exif_timestamp = metadata['timestamp']

        if exif_timestamp:
            print('[Process-File] exif found, timestamp = ' + exif_timestamp)
            assert len(exif_timestamp) == ImportSettings.TIMESTAMP_LEN
            process_image(companion_files, exif_timestamp, imported_images, local_filepath, pg_hook, rejected_files,
                          smb_hook_collection)
        else:
            print('[Process-File] exif not found')


def process_image(companion_files, exif_timestamp, imported_images, local_filepath, pg_hook, rejected_files,
                  smb_hook_collection):
    basename = os.path.basename(local_filepath)
    with open(local_filepath, mode='rb') as landing_file:
        file_hash = hashlib.file_digest(landing_file, "md5").hexdigest()
        assert file_hash
        metadata_found: bool = len(pg_hook.get_records(
            "select 1 "
            "from edm.metadata m "
            "join edm.tree t on t.abs_filename = m.abs_filename "
            f"where t.\"type\" = 'collection' and m.hash = '{file_hash}' "
        )) > 0
        if metadata_found:
            print('[Process-File][Process-Image] metadata found in DB, rejecting as duplicate')
            rejected_files.append(reject_file(basename, local_filepath))
        else:
            print(f'[Process-File][Process-Image] no duplicates found in DB, ready to import ')
            import_image(basename, companion_files, exif_timestamp, imported_images, local_filepath,
                         smb_hook_collection)


def import_image(basename, companion_files, exif_timestamp, imported_images, local_filepath, smb_hook_collection):
    print(f'[Process-File][Process-Image][Importing] mkdir {exif_timestamp}')
    with ignorednr(OSError, errno.EEXIST):
        smb_hook_collection.mkdir(exif_timestamp)
    print(f'[Process-File][Process-Image][Importing] import to {exif_timestamp}')
    imported_images.append(import_file(basename, local_filepath, smb_hook_collection, exif_timestamp))
    print('[Process-File][Process-Image][Importing] ready to process companion files')
    process_companions(local_filepath, smb_hook_collection, exif_timestamp, companion_files)


def process_companions(parent_local_filepath, smb_hook_collection, parent_timestamp, companion_files):
    local_filepath_no_ext, _ = os.path.splitext(parent_local_filepath)
    for extension in ImportSettings.COMPANION_FILE_EXTENSIONS:
        local_filepath = local_filepath_no_ext + extension
        print(f'[Process-File][Process-Image][Importing][Companions] looking for "{local_filepath}"')
        if os.path.exists(local_filepath) and os.path.isfile(local_filepath):
            print('[Process-File][Process-Image][Importing][Companions] companion file exists')
            basename = os.path.basename(local_filepath)
            print('[Process-File][Process-Image][Importing][Companions] importing companion file')
            companion_files.append(import_file(basename, local_filepath, smb_hook_collection, parent_timestamp))
        else:
            print('[Process-File][Process-Image][Importing][Companions] companion file not found')


def reject_file(basename, path) -> ProcessedFile:
    new_path = Variable.get(VariableName.LP_REJECTED) + '/' + basename
    print(f'[Reject-File] moving to rejected files as "{new_path}"')
    shutil.move(path, new_path)
    return ProcessedFile(path, new_path)


def import_file(basename, local_filepath, smb_hook_collection, timestamp) -> ProcessedFile:
    destination_filepath = timestamp + "/" + basename
    print(f'[Import-File] copying local {local_filepath} to remote "{destination_filepath}"')
    smb_hook_collection.push_from_local(destination_filepath=destination_filepath, local_filepath=local_filepath)
    print(f'[Import-File] removing local {local_filepath}')
    os.remove(local_filepath)
    return ProcessedFile(local_filepath, destination_filepath)


def tg_notif(**kwargs):
    ti = kwargs['ti']
    xcom = dict(ti.xcom_pull(dag_id=DagName.INGEST_LANDING_FILES, task_ids=PROCESS_EACH_LANDING_FILE_TASK_ID))
    msg1 = 'Imported Images: ' + str(xcom['count_imported_images'])
    msg2 = 'Imported Companion Files: ' + str(xcom['count_companion_files'])
    msg3 = 'Rejected Files: ' + str(xcom['count_rejected_files'])
    msg = '\n'.join([msg1, msg2, msg3])
    bot = telebot.TeleBot(Variable.get(VariableName.TG_TOKEN))
    for user_id in TG_USER_IDS:
        bot.send_message(user_id, msg)


with DAG(dag_id=DagName.PROCESS_LANDING_FILES, max_active_runs=1, schedule=SCHEDULE_MANUAL) as dag:
    dwh_refresh_before = TriggerDagRunOperator(
        task_id="trigger_" + DagName.REFRESH_STORAGE_TREE_INDEX + "_before",
        trigger_dag_id=DagName.REFRESH_STORAGE_TREE_INDEX,
        wait_for_completion=True)
    process_each_landing_file = PythonOperator(
        task_id=PROCESS_EACH_LANDING_FILE_TASK_ID,
        python_callable=process_each_landing_file)
    dwh_refresh_after = TriggerDagRunOperator(
        task_id="trigger_" + DagName.REFRESH_STORAGE_TREE_INDEX + "_after",
        trigger_dag_id=DagName.REFRESH_STORAGE_TREE_INDEX,
        wait_for_completion=False),
    tg_notif = PythonOperator(
        task_id="tg_notif",
        python_callable=tg_notif)
    dwh_refresh_before >> process_each_landing_file >> dwh_refresh_after >> tg_notif
