import io
from datetime import datetime

import requests
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import asset, Asset

from fdwh_config import Conn, VariableName, AssetName


@asset(name=AssetName.ADD_AI_DESCR_COLLECTION,
       schedule=[
           Asset(AssetName.AI_DESCR_HELPER_AVAIL),
           Asset(AssetName.MISSING_AI_DESCR_COLLECTION)
       ])
def add_missing_ai_descr_collection(context: dict, fdwh_ai_descr_helper_avail: Asset,
                                    fdwh_missing_aidc: Asset) -> None:
    avail: bool = context["inlet_events"][fdwh_ai_descr_helper_avail][-1].source_task_instance.xcom_pull()
    if avail:
        items = context["inlet_events"][fdwh_missing_aidc][-1].source_task_instance.xcom_pull()
        if len(items) > 0:
            pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
            for item in items:
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
