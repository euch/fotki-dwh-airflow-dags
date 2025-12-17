import base64
import io
from datetime import timedelta

import requests
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, Variable, dag, task
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from pendulum import Timezone

from fdwh_config import *
from fdwh_op_check_helper_available import CheckHelperAvailableOperator

schedule = AssetOrTimeSchedule(
    timetable=CronTriggerTimetable('0 1 * * *', timezone=Timezone(server_tz_name)),
    assets=Asset(AssetName.CORE_METADATA_UPDATED)
)
max_duration = timedelta(hours=6)
tags = {
    DagTag.FDWH_CORE,
    DagTag.FDWH_HELPERS,
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
    assert_ollama_available = CheckHelperAvailableOperator(
        task_id='assert_ollama_available',
        url=Variable.get(VariableName.OLLAMA_ENDPOINT))

    @task()
    def add_missing_caption_collection():
        return _add_missing_caption('collection')

    @task()
    def add_missing_caption_archive():
        return _add_missing_caption('archive')

    @task(outlets=[Asset(AssetName.CORE_CAPTION_UPDATED)])
    def end():
        pass

    assert_ollama_available >> add_missing_caption_collection() >> add_missing_caption_archive() >> end()

dag()


def _add_missing_caption(tree_type: str):
    pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
    endpoint = Variable.get(VariableName.OLLAMA_ENDPOINT)

    while True:
        records = pg_hook.get_records(_missing_caption_select_sql, parameters=[tree_type])

        if len(records) == 0:
            return

        cc_id, model, prompt = pg_hook.get_records(_caption_conf_select_sql)[0]
        print(model)

        for r in records:
            _hash, preview, abs_filename, has_more_records = r[0], r[1], r[2], r[3]
            print(_hash)
            image_base64 = base64.b64encode(io.BytesIO(preview).read()).decode('utf-8')
            payload = {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "images": [image_base64]
            }
            headers = {"Content-Type": "application/json"}
            print(f"posting image data of {abs_filename} ({_hash}) to {endpoint}")
            response = requests.post(endpoint, json=payload, headers=headers)
            if response.status_code == 200:
                result = response.json()
                captions = result.get('response')
                pg_hook.run(_caption_insert_sql, parameters=[_hash, cc_id, captions])
                pg_hook.run(_log_update_sql, parameters=[abs_filename])
            else:
                print(response.content)
                raise AirflowException(f'Helper returned {response.status_code} for {abs_filename}')

            if not has_more_records:
                return
