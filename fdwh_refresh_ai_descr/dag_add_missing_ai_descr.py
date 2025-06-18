import io
from datetime import datetime

import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, Variable, dag, task

from fdwh_config import *


@dag(max_active_runs=1,
     default_args=dag_default_args,
     schedule=[
         Asset(AssetName.AI_DESCR_HELPER_AVAIL),
         Asset(AssetName.METADATA_UPDATED_COLLECTION)
     ],
     )
def add_missing_ai_descr():
    find_missing_ai_descr_collection = SQLExecuteQueryOperator(
        task_id='find_missing_ai_descr_collection',
        conn_id=Conn.POSTGRES,
        sql='sql/find_missing_ai_descr_collection.sql',
        do_xcom_push=True)

    @task(outlets=[Asset(AssetName.AI_DESCR_UPDATED_COLLECTION)])
    def add_missing_ai_descr_collection(**context) -> None:
        missing_items = context["ti"].xcom_pull(task_ids="find_missing_ai_descr_collection", key="return_value")
        if len(missing_items) > 0:
            pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
            for item in missing_items:
                abs_filename = item[0]
                sql_query = f'SELECT preview FROM edm.metadata WHERE abs_filename = %s and preview is not null;'
                query_result = pg_hook.get_records(sql_query, parameters=[abs_filename])
                if query_result:
                    binary_data = query_result[0][0]
                    flo = io.BytesIO(binary_data)
                    files = {'file': flo}
                    response = requests.post(Variable.get(VariableName.AI_DESCR_ENDPOINT), files=files)
                    if response.status_code == 200:
                        captions = response.json()["description"]
                        sql = f'''
                                        INSERT INTO edm.ai_description (abs_filename, caption_vit_gpt2)
                                        VALUES (%s, %s)
                                        ON CONFLICT (abs_filename) 
                                        DO UPDATE SET caption_vit_gpt2 = %s;
                                        '''
                        pg_hook.run(sql, parameters=[abs_filename, captions, captions])
                        pg_hook.run("update log.edm_log set ai_description_add_ts = %s where abs_filename = %s",
                                    parameters=(datetime.now(), abs_filename))
                    else:
                        print(f"Failed to parse preview bytes. abs_filename = " + abs_filename)

    find_missing_ai_descr_collection >> add_missing_ai_descr_collection()


add_missing_ai_descr()
