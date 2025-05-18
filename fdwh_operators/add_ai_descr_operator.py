import io
from datetime import datetime

import requests
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class AddAiDescrVitGpt2Operator(BaseOperator):

    def __init__(self,
                 xcom_key: str,
                 pg_conn_name: str,
                 ai_descr_endpoint: str,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.xcom_key = xcom_key
        self.pg_conn_name = pg_conn_name
        self.ai_descr_endpoint = ai_descr_endpoint

    def execute(self, context):
        items: list[str] = context['task_instance'].xcom_pull(dag_id=self.dag_id,
                                                              task_ids=self.xcom_key,
                                                              key='return_value')
        pg_hook = PostgresHook.get_hook(self.pg_conn_name)
        for item in items:
            abs_filename = item[0]
            sql_query = f'SELECT preview FROM edm.metadata WHERE abs_filename = %s and preview is not null;'
            query_result = pg_hook.get_records(sql_query, parameters=[abs_filename])
            if query_result:
                binary_data = query_result[0][0]
                flo = io.BytesIO(binary_data)
                files = {'file': flo}
                response = requests.post(self.ai_descr_endpoint, files=files)
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
