import json

from airflow.sdk import dag, task

from assets import CORE_METADATA_UPDATED, CORE_TREE_UPDATED
from core import metadata_get, file_get, metadata_select_missing, metadata_insert, metadata_update_log, ImageMetadata


@dag(max_active_runs=1, schedule=[CORE_TREE_UPDATED], default_args={'retries': 0})
def core_metadata_update():
    @task(outlets=[CORE_METADATA_UPDATED])
    def add_missing_metadata():
        no_file: set[str] = set()
        no_exif: set[str] = set()
        no_preview: set[str] = set()
        total = 0

        def result_dict() -> str:
            return json.dumps(
                obj={
                    'total': total,
                    'no_file': list(no_file),
                    'no_exif': list(no_exif),
                    'no_preview': list(no_preview),
                },
                indent=4)

        while True:
            records = metadata_select_missing(list(no_file))

            if not records:
                return result_dict()

            for r in records:
                abs_filename, has_more_records = r[0], r[1]
                file = file_get(abs_filename)

                if not file:
                    no_file.add(abs_filename)
                else:
                    metadata: ImageMetadata = metadata_get(abs_filename, file)

                    if not metadata.exif:
                        no_exif.add(abs_filename)

                    if not metadata.preview_base64:
                        no_preview.add(abs_filename)

                    metadata_insert(abs_filename, metadata)
                    metadata_update_log(abs_filename, metadata.hash)

                total += 1

                if not has_more_records:
                    return result_dict()

    add_missing_metadata()


core_metadata_update()
