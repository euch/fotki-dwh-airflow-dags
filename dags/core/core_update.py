import base64
import io
import json
import os

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.timetables.trigger import CronTriggerTimetable

from assets import CORE_METADATA_UPDATED, CORE_CAPTION_UPDATED
from assets import CORE_TREE_DIFF_FOUND
from assets import CORE_TREE_UPDATED
from connections import POSTGRES
from core import get_captions, caption_insert, caption_update_log, caption_select_missing
from core import metadata_get, metadata_select_missing, metadata_insert, metadata_update_log, ImageMetadata


@dag(max_active_runs=1, schedule=CronTriggerTimetable('*/5 * * * *', timezone='UTC'), default_args={'retries': 0})
def core_tree_diff_wait():
    SqlSensor(
        task_id='tree_diff_sensor',
        conn_id=POSTGRES,
        mode='poke',
        outlets=[CORE_TREE_DIFF_FOUND],
        poke_interval=60 * 5,  # Check every 5 minutes
        soft_fail=True,
        sql='sql/tree_select_diff.sql',
        timeout=60 * 60 * 24)  # Timeout after 24 hour


@dag(max_active_runs=1, schedule=[CORE_TREE_DIFF_FOUND], default_args={'retries': 0})
def core_tree_update():
    tree_delete_old = SQLExecuteQueryOperator(
        task_id='tree_delete_old',
        conn_id=POSTGRES,
        sql='sql/tree_delete_old.sql')

    tree_insert_new = SQLExecuteQueryOperator(
        task_id='tree_insert_new',
        conn_id=POSTGRES,
        sql='sql/tree_insert_new.sql',
        outlets=[CORE_TREE_UPDATED])

    tree_delete_old >> tree_insert_new


@dag(max_active_runs=1, schedule=[CORE_TREE_UPDATED], default_args={'retries': 0})
def core_metadata_update():
    @task(outlets=[CORE_METADATA_UPDATED])
    def add_missing_metadata():
        metadata_service_error: set[str] = set()
        total = 0

        def result_dict() -> str:
            return json.dumps(
                obj={
                    'total': total,
                    'metadata_service_error': list(metadata_service_error),
                },
                indent=4)

        while True:
            records = metadata_select_missing(list(metadata_service_error))

            if not records:
                return result_dict()

            for r in records:
                abs_filename, has_more_records = r[0], r[1]
                metadata: ImageMetadata | None = metadata_get(abs_filename)

                if metadata:
                    metadata_insert(abs_filename, metadata)
                    metadata_update_log(abs_filename, metadata.hash)
                else:
                    metadata_service_error.add(abs_filename)

                total += 1

                if not has_more_records:
                    return result_dict()

    add_missing_metadata()


@dag(max_active_runs=1, schedule=[CORE_METADATA_UPDATED], default_args={'retries': 0})
def core_caption_update():
    @task
    def get_caption_conf():
        with open(os.path.join(os.path.dirname(__file__), 'sql', 'caption_select_conf.sql'), 'r') as f:
            row = PostgresHook.get_hook(POSTGRES).get_records(f.read())[0]
        return {
            "caption_conf_id": row[0],
            "model": row[1],
            "prompt": row[2]
        }

    @task(outlets=[CORE_CAPTION_UPDATED])
    def add_missing_caption(caption_conf: dict):
        total = 0

        def result_dict() -> str:
            return json.dumps(
                obj={
                    'total': total,
                },
                indent=4)

        while True:
            records = caption_select_missing()

            if not records:
                return result_dict()

            for _hash, preview, abs_filename, has_more_records in records:
                image_base64 = base64.b64encode(io.BytesIO(preview).read()).decode('utf-8')
                captions = get_captions(caption_conf, image_base64)
                caption_insert(_hash, caption_conf, captions)
                caption_update_log(abs_filename)
                total += 1

                if not has_more_records:
                    return result_dict()

    add_missing_caption(get_caption_conf())


core_tree_diff_wait()
core_tree_update()
core_metadata_update()
core_caption_update()
