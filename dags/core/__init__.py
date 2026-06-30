import base64
import json
import os

import psycopg2 as psql
import requests
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

from connections import POSTGRES
from variables import ollama_endpoint, get_file_endpoint, metadata_endpoint


def file_get(file_path: str) -> bytes:
    url = get_file_endpoint().rstrip('/') + file_path
    resp = requests.get(url)
    resp.raise_for_status()  # raise an exception for 4xx/5xx codes
    return resp.content


def metadata_select_missing():
    with open(os.path.join(os.path.dirname(__file__), 'sql', 'metadata_select_missing.sql'), 'r') as f:
        pg_hook = PostgresHook.get_hook(POSTGRES)
        return pg_hook.get_records(f.read())


def metadata_get(abs_filename: str, file) -> dict[str, object]:
    resp = requests.post(metadata_endpoint(), files={'file': file}, data={'filename': abs_filename})
    resp.raise_for_status()  # raise an exception for 4xx/5xx codes
    return resp.json()


def metadata_insert(abs_filename: str, metadata: dict[str, object]):
    with open(os.path.join(os.path.dirname(__file__), 'sql', 'metadata_insert.sql'), 'r') as f:
        pg_hook = PostgresHook.get_hook(POSTGRES)
        exif = None if metadata['exif'] is None else json.dumps(metadata['exif'])
        preview = None if metadata['preview'] is None else psql.Binary(base64.b64decode(metadata['preview']))
        return pg_hook.run(f.read(), parameters=[abs_filename, metadata['hash'], exif, preview])


def metadata_update_log(abs_filename: str, metadata: dict[str, object]):
    with open(os.path.join(os.path.dirname(__file__), 'sql', 'metadata_update_log.sql'), 'r') as f:
        pg_hook = PostgresHook.get_hook(POSTGRES)
        return pg_hook.run(f.read(), parameters=(metadata['hash'], abs_filename))


def get_captions(caption_conf, image_base64):
    payload = {
        "model": caption_conf['model'],
        "prompt": caption_conf['prompt'],
        "stream": False,
        "images": [image_base64]
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(ollama_endpoint(), json=payload, headers=headers)
    if response.status_code == 200:
        result = response.json()
        captions = result.get('response')
        return captions
    else:
        print(response.content)
        raise AirflowException(f'Helper returned {response.status_code}')
