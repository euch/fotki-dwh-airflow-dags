import io
from datetime import datetime

import requests
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, Variable, dag, task

from fdwh_config import *
from fdwh_op_check_helper_available import CheckHelperAvailableOperator

schedule = [Asset(AssetName.CORE_METADATA_UPDATED)]
tags = {
    DagTag.FDWH_CORE,
    DagTag.FDWH_HELPERS,
    DagTag.PG,
    DagTag.SMB,
}

missing_ai_descr_select_sql = '''
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
join log.core_log cl on
    cl.abs_filename = m.abs_filename
join core.tree t on
    t.abs_filename = m.abs_filename
where
    ad.abs_filename is null
    and m.preview is not null
    and t."type" = %s
    and t.size < 100000000 -- up to 100 MB limit
order by
    cl.tree_add_ts desc
limit 5;
'''

insert_ai_descr_sql = f'''
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

update_log_sql = '''
update
	log.core_log
set
	ai_description_add_ts = %s
where
	abs_filename = %s
'''


@dag(max_active_runs=1, default_args=dag_args_retry, schedule=schedule, tags=tags)
def add_missing_ai_descr():
    assert_ai_descr_helper_available = CheckHelperAvailableOperator(
        task_id='assert_ai_descr_helper_available',
        url=Variable.get(VariableName.AI_DESCR_ENDPOINT))

    @task
    def add_missing_ai_descr_collection():
        _add_missing_ai_descr('collection')

    @task
    def add_missing_ai_descr_archive():
        _add_missing_ai_descr('archive')

    @task
    def add_missing_ai_descr_trash():
        _add_missing_ai_descr('trash')

    @task(outlets=[Asset(AssetName.CORE_AI_DESCR_UPDATED)])
    def end():
        pass

    assert_ai_descr_helper_available >> add_missing_ai_descr_collection() >> add_missing_ai_descr_archive() >> add_missing_ai_descr_trash() >> end()


add_missing_ai_descr()


def _add_missing_ai_descr(tree_type: str):
    pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
    endpoint = Variable.get(VariableName.AI_DESCR_ENDPOINT)
    while True:
        records = pg_hook.get_records(missing_ai_descr_select_sql, parameters=[tree_type])
        if not records:
            return

        for r in records:
            abs_filename, preview, has_more_records = r[0], r[1], r[2]
            print(abs_filename)
            response = requests.post(endpoint, files={'file': io.BytesIO(preview)})
            if response.status_code == 200:
                captions = response.json()["description"]
                pg_hook.run(insert_ai_descr_sql, parameters=[abs_filename, captions, captions])
                pg_hook.run(update_log_sql, parameters=(datetime.now(), abs_filename))
            else:
                raise AirflowException(f'Helper returned {response.status_code} for {abs_filename}')

            if not has_more_records:
                return
