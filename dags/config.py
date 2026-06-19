dag_args_noretry = {
    'retries': 0,
}

dag_args_retry = {
    'retries': 10,
}


class Conn:
    POSTGRES = 'postgres'
    MINIO = 'minio'

    SMB_ARCHIVE = "storage_smb_archive"
    SMB_COLLECTION = "storage_smb_collection"
    SMB_TRASH = "storage_smb_trash"


class DagTag:
    CLEANUP = 'cleanup'
    MONITORING = 'monitoring'
    PG = 'pg'
    SMB = 'smb'

    DWH_RAW = 'dwh_raw'
    DWH_CORE = 'dwh_core'
    DWH_MARTS = 'dwh_marts'

    DUPLICATES = 'duplicates'
    HELPERS = 'helpers'
    STORAGE_IO = 'storage_io'


class AssetName:
    CORE_TREE_UPDATED = 'core_tree_updated'
    CORE_METADATA_UPDATED = 'core_metadata_updated'
    CORE_CAPTION_UPDATED = 'core_caption_updated'


class ImportSettings:
    TIMESTAMP_FMT = '%Y-%m-%d-auto'


class VariableName:
    STORAGE_PATH_ARCHIVE = 'RP_ARCHIVE'
    STORAGE_PATH_COLLECTION = 'RP_COLLECTION'
    STORAGE_PATH_TRASH = 'RP_TRASH'

    OLLAMA_ENDPOINT = 'OLLAMA_ENDPOINT'
    METADATA_ENDPOINT = 'METADATA_ENDPOINT'
    EXIF_TS_ENDPOINT = 'EXIF_TS_ENDPOINT'
