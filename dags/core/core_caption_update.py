import base64
import io
from enum import Enum

import requests
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Asset, Variable, dag, task
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

from config import *

schedule = AssetOrTimeSchedule(
    timetable=CronTriggerTimetable("0 23 * * *", timezone='UTC'),
    assets=Asset(AssetName.CORE_METADATA_UPDATED))
tags = {
    DagTag.DWH_CORE,
    DagTag.HELPERS,
    DagTag.PG,
    DagTag.SMB,
}
dag_id = 'core_caption_update'

_missing_caption_select_sql = '''
select
    distinct on (m.hash)
    m.hash,
    m.preview,
    m.abs_filename,
    CASE 
        WHEN COUNT(*) OVER() >= 5 THEN true 
        ELSE false 
    END as has_more_pages
from
    core.metadata m
left join core.caption c on
    c.hash = m.hash
join core.tree t on
    t.abs_filename = m.abs_filename
where
    c.hash is null
    and m.preview is not null
    and t."type" = %s
order by
    m.hash, m.abs_filename desc
limit 5;
'''

_caption_conf_select_sql = '''
select id, model, prompt from core.current_caption_conf;
'''

_caption_insert_sql = f'''
insert
	into
	core.caption
(hash, caption_conf_id,	caption)
values(%s, %s, %s);
'''

_log_update_sql = '''
update
	log.core_log
set
	caption_add_ts = now()
where
	abs_filename = %s
'''


class CaptionStatus(str, Enum):
    NO_RECORDS_FOUND = 'no_records_found'
    HAS_MORE_RECORDS = 'has_more_records'
    NO_RECORDS_LEFT = 'no_records_left'

    def __str__(self):
        return str(self.value)


@dag(dag_id=dag_id, max_active_runs=1, default_args=dag_args_noretry, schedule=schedule, tags=tags)
def core_caption_update():
    @task
    def get_caption_conf():
        pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
        row = pg_hook.get_records(_caption_conf_select_sql)[0]
        return {
            "caption_conf_id": row[0],
            "model": row[1],
            "prompt": row[2]
        }

    @task()
    def add_missing_caption_collection(settings: dict):
        return _add_missing_caption(settings, 'collection', 1000)

    @task()
    def add_missing_caption_archive(settings: dict):
        return _add_missing_caption(settings, 'archive', 100)

    @task()
    def add_missing_caption_trash(settings: dict):
        return _add_missing_caption(settings, 'trash', 100)

    @task.short_circuit()
    def has_more_records(results: list[CaptionStatus]) -> bool:
        for item in results:
            if item == CaptionStatus.HAS_MORE_RECORDS:
                return True

        return False

    trigger_again = TriggerDagRunOperator(task_id='trigger_again', trigger_dag_id=dag_id)

    @task.short_circuit()
    def some_records_processed(results: list[CaptionStatus]):
        for item in results:
            if item == CaptionStatus.HAS_MORE_RECORDS or item == CaptionStatus.NO_RECORDS_LEFT:
                return True

        return False

    create_asset = EmptyOperator(task_id='create_asset', outlets=[Asset(AssetName.CORE_CAPTION_UPDATED)])

    caption_conf = get_caption_conf()

    process_collection = add_missing_caption_collection(caption_conf)
    process_archive = add_missing_caption_archive(caption_conf)
    process_trash = add_missing_caption_trash(caption_conf)

    process_collection >> process_archive >> process_trash

    has_more_records([process_collection, process_archive, process_trash]) >> trigger_again

    some_records_processed([process_collection]) >> create_asset


core_caption_update()


def _add_missing_caption(caption_conf: dict, tree_type: str, limit: int) -> CaptionStatus:
    pg_hook: PostgresHook = PostgresHook.get_hook(Conn.POSTGRES)
    processed_count = 0

    while True:
        records = pg_hook.get_records(_missing_caption_select_sql, parameters=[tree_type])

        if len(records) == 0:
            return CaptionStatus.NO_RECORDS_FOUND

        for _hash, preview, abs_filename, has_more_records in records:
            image_base64 = base64.b64encode(io.BytesIO(preview).read()).decode('utf-8')
            captions = _get_captions(caption_conf, image_base64)
            pg_hook.run(_caption_insert_sql, parameters=[_hash, caption_conf['caption_conf_id'], captions])
            pg_hook.run(_log_update_sql, parameters=[abs_filename])
            processed_count += 1

            if has_more_records:
                if processed_count < limit:
                    continue
                else:
                    return CaptionStatus.HAS_MORE_RECORDS
            else:
                return CaptionStatus.NO_RECORDS_LEFT


def _get_captions(caption_conf, image_base64):
    endpoint = Variable.get(VariableName.OLLAMA_ENDPOINT)
    payload = {
        "model": caption_conf['model'],
        "prompt": caption_conf['prompt'],
        "stream": False,
        "images": [image_base64]
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(endpoint, json=payload, headers=headers)
    if response.status_code == 200:
        result = response.json()
        captions = result.get('response')
        return captions
    else:
        print(response.content)
        raise AirflowException(f'Helper returned {response.status_code}')
