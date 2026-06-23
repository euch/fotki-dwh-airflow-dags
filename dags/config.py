dag_args_noretry = {
    'retries': 0,
}


class Conn:
    POSTGRES = 'postgres'


class AssetName:
    CORE_UPDATED = 'core_updated'


class VariableName:
    EXIF_TS_ENDPOINT = 'EXIF_TS_ENDPOINT'
    GET_FILE_ENDPOINT = 'GET_FILE_ENDPOINT'
    METADATA_ENDPOINT = 'METADATA_ENDPOINT'
    OLLAMA_ENDPOINT = 'OLLAMA_ENDPOINT'
