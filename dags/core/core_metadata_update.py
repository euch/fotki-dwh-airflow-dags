import base64
import json
import os
from typing import Any

import psycopg2 as psql
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task, Variable

from config import *
from core import DagId


def file_get(file_path: str) -> bytes:
    server_url = Variable.get(VariableName.GET_FILE_ENDPOINT)
    url = server_url.rstrip('/') + file_path
    resp = requests.get(url)
    resp.raise_for_status()  # raise an exception for 4xx/5xx codes
    return resp.content


def metadata_select_missing() -> Any:
    with open(os.path.join(os.path.dirname(__file__), 'sql', 'metadata_select_missing.sql'), 'r') as f:
        pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
        return pg_hook.get_records(f.read())


def metadata_get(abs_filename: str, file) -> dict[str, object]:
    server_url = Variable.get(VariableName.METADATA_ENDPOINT)
    resp = requests.post(server_url, files={'file': file}, data={'filename': abs_filename})
    resp.raise_for_status()  # raise an exception for 4xx/5xx codes
    return resp.json()


def metadata_insert(abs_filename: str, metadata: dict[str, object]) -> Any:
    with open(os.path.join(os.path.dirname(__file__), 'sql', 'metadata_insert.sql'), 'r') as f:
        pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
        exif = None if metadata['exif'] is None else json.dumps(metadata['exif'])
        preview = None if metadata['preview'] is None else psql.Binary(base64.b64decode(metadata['preview']))
        return pg_hook.run(f.read(), parameters=[abs_filename, metadata['hash'], exif, preview])


def metadata_update_log(abs_filename: str, metadata: dict[str, object]) -> Any:
    with open(os.path.join(os.path.dirname(__file__), 'sql', 'metadata_update_log.sql'), 'r') as f:
        pg_hook = PostgresHook.get_hook(Conn.POSTGRES)
        return pg_hook.run(f.read(), parameters=(metadata['hash'], abs_filename))


@dag(dag_id=DagId.CORE_METADATA_UPDATE, max_active_runs=1, default_args=dag_args_noretry)
def dag():
    @task
    def add_missing_metadata():
        no_exif: set[str] = set()
        no_hash: set[str] = set()
        no_preview: set[str] = set()
        total = 0

        def result_dict() -> str:
            return json.dumps(
                obj={
                    'total': total,
                    'no_exif': list(no_exif),
                    'no_hash': list(no_hash),
                    'no_preview': list(no_preview),
                },
                indent=4)

        while True:
            records = metadata_select_missing()

            if not records:
                return result_dict()

            for r in records:
                abs_filename, has_more_records = r[0], r[1]

                metadata = metadata_get(abs_filename, file_get(abs_filename))
                if not metadata['hash']:
                    no_hash.add(abs_filename)

                if not metadata['exif']:
                    no_exif.add(abs_filename)

                if not metadata['preview']:
                    no_preview.add(abs_filename)

                metadata_insert(abs_filename, metadata)
                metadata_update_log(abs_filename, metadata)
                total += 1

                if not has_more_records:
                    return result_dict()

    add_missing_metadata()


dag()
