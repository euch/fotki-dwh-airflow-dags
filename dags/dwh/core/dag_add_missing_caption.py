import base64
import io
from datetime import timedelta

import requests
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, Variable, dag, task
from airflow.timetables.trigger import DeltaTriggerTimetable

from config import *

schedule = DeltaTriggerTimetable(timedelta(minutes=1))
tags = {
    DagTag.DWH_CORE,
    DagTag.HELPERS,
    DagTag.PG,
    DagTag.SMB,
}

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


@dag(dag_id=DagName.ADD_MISSING_CAPTION, max_active_runs=1, default_args=dag_args_noretry,
     schedule=schedule, tags=tags)
def dag():
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

    @task(outlets=[Asset(AssetName.CORE_CAPTION_UPDATED)])
    def end():
        pass

    caption_conf = get_caption_conf()
    process_collection = add_missing_caption_collection(caption_conf)
    process_archive = add_missing_caption_archive(caption_conf)
    process_collection >> process_archive >> end()


dag()


def _add_missing_caption(caption_conf: dict, tree_type: str, limit: int) -> list[str]:
    processed_abs_filenames = []
    pg_hook: PostgresHook = PostgresHook.get_hook(Conn.POSTGRES)

    while True:
        records = pg_hook.get_records(_missing_caption_select_sql, parameters=[tree_type])

        if len(records) == 0:
            return processed_abs_filenames

        for _hash, preview, abs_filename, has_more_records in records:
            image_base64 = base64.b64encode(io.BytesIO(preview).read()).decode('utf-8')
            captions = _get_captions(caption_conf, image_base64)
            pg_hook.run(_caption_insert_sql, parameters=[_hash, caption_conf['caption_conf_id'], captions])
            pg_hook.run(_log_update_sql, parameters=[abs_filename])
            processed_abs_filenames.append(abs_filename)

            if not has_more_records or len(processed_abs_filenames) >= limit:
                return processed_abs_filenames


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
