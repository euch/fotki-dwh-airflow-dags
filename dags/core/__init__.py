import base64
import json
import os
from dataclasses import dataclass

import requests
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import Binary
from requests import HTTPError

from connections import POSTGRES
from variables import ollama_endpoint, metadata_endpoint


def pg_run(filename: str, parameters: list[object]):
    with open(os.path.join(os.path.dirname(__file__), 'sql', filename), 'r') as f:
        pg_hook: PostgresHook = PostgresHook.get_hook(POSTGRES)
        return pg_hook.run(f.read(), parameters=parameters)


def pg_get(filename: str, parameters: tuple | list | dict | None = None):
    with open(os.path.join(os.path.dirname(__file__), 'sql', filename), 'r') as f:
        pg_hook: PostgresHook = PostgresHook.get_hook(POSTGRES)
        return pg_hook.get_records(f.read(), parameters=parameters)


def metadata_select_missing(ignore_abs_filenames: list[str]):
    return pg_get('metadata_select_missing.sql', parameters=[ignore_abs_filenames])


@dataclass
class ImageMetadata:
    hash: str
    exif: dict[str, object] | None
    preview_base64: str | None


def metadata_get(abs_filename: str) -> ImageMetadata | None:
    resp = requests.get(metadata_endpoint(), json={'path': abs_filename})
    try:
        resp.raise_for_status()  # raise an exception for 4xx/5xx codes
        metadata = resp.json()
        return ImageMetadata(metadata['hash'], metadata['exif'], metadata['preview'])
    except HTTPError as e:
        print(e)
        return None


def metadata_insert(abs_filename: str, metadata: ImageMetadata):
    exif = None if metadata.exif is None else json.dumps(metadata.exif)
    preview = None if metadata.preview_base64 is None else Binary(base64.b64decode(metadata.preview_base64))
    return pg_run('metadata_insert.sql', [abs_filename, metadata.hash, exif, preview])


def metadata_update_log(abs_filename: str, _hash: str):
    return pg_run('metadata_update_log.sql', [_hash, abs_filename])


def caption_select_missing():
    pg_get('caption_select_missing.sql')


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


def caption_insert(_hash, caption_conf: dict, captions):
    pg_run('caption_insert.sql', [_hash, caption_conf['caption_conf_id'], captions])


def caption_update_log(abs_filename):
    pg_run('caption_update_log.sql', [abs_filename])
