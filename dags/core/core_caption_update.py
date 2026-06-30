import base64
import io
import json
import os

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from assets import CORE_METADATA_UPDATED, CORE_CAPTION_UPDATED
from connections import POSTGRES
from core import get_captions


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
        pg_hook: PostgresHook = PostgresHook.get_hook(POSTGRES)
        total = 0

        def result_dict() -> str:
            return json.dumps(
                obj={
                    'total': total,
                },
                indent=4)

        while True:
            with open(os.path.join(os.path.dirname(__file__), 'sql', 'caption_select_missing.sql'), 'r') as f:
                records = pg_hook.get_records(f.read())

            if not records:
                return result_dict()

            for _hash, preview, abs_filename, has_more_records in records:
                image_base64 = base64.b64encode(io.BytesIO(preview).read()).decode('utf-8')
                captions = get_captions(caption_conf, image_base64)
                with open(os.path.join(os.path.dirname(__file__), 'sql', 'caption_insert.sql'), 'r') as f:
                    pg_hook.run(f.read(), parameters=[_hash, caption_conf['caption_conf_id'], captions])
                with open(os.path.join(os.path.dirname(__file__), 'sql', 'caption_update_log.sql'), 'r') as f:
                    pg_hook.run(f.read(), parameters=[abs_filename])
                total += 1

                if not has_more_records:
                    return result_dict()

    add_missing_caption(get_caption_conf())


core_caption_update()
