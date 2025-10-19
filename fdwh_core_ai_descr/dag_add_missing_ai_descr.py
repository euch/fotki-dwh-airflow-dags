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
    timetable=CronTriggerTimetable('0 1 * * *', timezone=Timezone(server_tz)),
    assets=Asset(AssetName.CORE_METADATA_UPDATED)
)
max_duration = timedelta(hours=6)
tags = {
    DagTag.FDWH_CORE,
    DagTag.FDWH_HELPERS,
    DagTag.PG,
    DagTag.SMB,
}

_missing_ai_descr_select_sql = '''
select 
    m.abs_filename,
    m.preview,
    CASE 
        WHEN COUNT(*) OVER() >= 5 THEN true 
        ELSE false 
    END as has_more_pages
from
    core.metadata m
left join core.ai_description ad on
    ad.abs_filename = m.abs_filename
join core.tree t on
    t.abs_filename = m.abs_filename
where
    ad.abs_filename is null
    and m.preview is not null
    and t."type" = %s
	and t.abs_filename not in %s    
order by
    m.abs_filename desc
limit 5;
'''

_insert_ai_descr_sql = f'''
insert
	into
	core.ai_description (abs_filename,
	caption_vit_gpt2)
values (%s,
%s)
on
conflict (abs_filename) 
do
update
set
	caption_vit_gpt2 = %s;
'''

_update_log_sql = '''
update
	log.core_log
set
	ai_description_add_ts = now()
where
	abs_filename = %s
'''


@dag(dag_id=DagName.ADD_MISSING_AI_DESCR, max_active_runs=1, default_args=dag_args_noretry,
     schedule=schedule, tags=tags)
def add_missing_ai_descr():
    assert_ai_descr_helper_available = CheckHelperAvailableOperator(
        task_id='assert_ai_descr_helper_available',
        url=Variable.get(VariableName.AI_DESCR_ENDPOINT))

    @task(execution_timeout=max_duration)
    def add_missing_ai_descr_collection():
        return _add_missing_ai_descr('collection')

    @task(execution_timeout=max_duration)
    def add_missing_ai_descr_archive():
        return _add_missing_ai_descr('archive')

    @task(execution_timeout=max_duration)
    def add_missing_ai_descr_trash():
        return _add_missing_ai_descr('trash')

    @task(outlets=[Asset(AssetName.CORE_AI_DESCR_UPDATED)])
    def end():
        pass

    assert_ai_descr_helper_available >> [
        add_missing_ai_descr_collection(),
        add_missing_ai_descr_trash(),
        add_missing_ai_descr_archive()
    ] >> end()


add_missing_ai_descr()


def _add_missing_ai_descr(tree_type: str):
    pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
    endpoint = Variable.get(VariableName.AI_DESCR_ENDPOINT)

    broken_previews_abs_filenames = {''}

    def result_dict() -> dict:
        return {
            'broken_previews': ','.join(broken_previews_abs_filenames),
        }

    while True:
        records = pg_hook.get_records(_missing_ai_descr_select_sql,
                                      parameters=[tree_type, tuple(broken_previews_abs_filenames)])
        if not records:
            return result_dict()

        for r in records:
            abs_filename, preview, has_more_records = r[0], r[1], r[2]
            print(abs_filename)
            if len(preview) > 1000:
                response = requests.post(endpoint, files={'file': io.BytesIO(preview)})
                if response.status_code == 200:
                    captions = response.json()["description"]
                    pg_hook.run(_insert_ai_descr_sql, parameters=[abs_filename, captions, captions])
                    pg_hook.run(_update_log_sql, parameters=[abs_filename])
                else:
                    raise AirflowException(f'Helper returned {response.status_code} for {abs_filename}')
            else:
                broken_previews_abs_filenames.add(abs_filename)
                print("Skipping: Preview is broken")

            if not has_more_records:
                return result_dict()
