import io
from datetime import datetime

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, Variable, dag, task

from fdwh_config import *
from fdwh_core_ai_descr.dto.add_ai_descr_item import AddAiDescrItem

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
    and t."type" = 'collection'
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


@dag(max_active_runs=1,
     default_args=dag_default_args,
     schedule=[
         Asset(AssetName.AI_DESCR_HELPER_AVAIL),
         Asset(AssetName.METADATA_UPDATED_COLLECTION)
     ],
     )
def add_missing_ai_descr():
    @task(outlets=[Asset(AssetName.AI_DESCR_UPDATED_COLLECTION)])
    def add_missing_ai_descr_collection() -> list[AddAiDescrItem]:
        pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
        res: list[AddAiDescrItem] = []
        endpoint = Variable.get(VariableName.AI_DESCR_ENDPOINT)
        while True:
            for r in pg_hook.get_records(missing_ai_descr_select_sql):
                abs_filename, preview, has_more_records = r[0], r[1], r[2]
                response = requests.post(endpoint, files={'file': io.BytesIO(preview)})
                if response.status_code == 200:
                    captions = response.json()["description"]
                    pg_hook.run(insert_ai_descr_sql, parameters=[abs_filename, captions, captions])
                    pg_hook.run(update_log_sql, parameters=(datetime.now(), abs_filename))
                    res.append(AddAiDescrItem(abs_filename))
                else:
                    print(f'Helper returned status code {response.status_code}')
                    res.append(AddAiDescrItem(abs_filename, error=str(response)))

                if not has_more_records:
                    return res

    add_missing_ai_descr_collection()


add_missing_ai_descr()
