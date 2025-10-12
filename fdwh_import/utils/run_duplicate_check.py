import hashlib
from io import BytesIO

from airflow.providers.postgres.hooks.postgres import PostgresHook


def run_duplicate_check(obj_bytes: BytesIO | None, pg_hook: PostgresHook) -> bool:
    md5_hash = get_md5_hash(obj_bytes)
    duplicate = hash_already_used(md5_hash, pg_hook)

    return duplicate


def get_md5_hash(obj_bytes: BytesIO | None) -> (str, BytesIO):
    md5_hash = hashlib.file_digest(obj_bytes, "md5").hexdigest()
    assert md5_hash
    return md5_hash


def hash_already_used(md5_hash, pg_hook: PostgresHook) -> bool:
    records = pg_hook.get_records(
        "select 1 "
        "from core.metadata m "
        "join core.tree t on t.abs_filename = m.abs_filename "
        f"where t.\"type\" = 'collection' and m.hash = '{md5_hash}' "
    )
    return len(records) > 0
