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


class AssetName:
    CORE_UPDATED = 'core_updated'


class VariableName:
    STORAGE_PATH_ARCHIVE = 'RP_ARCHIVE'
    STORAGE_PATH_COLLECTION = 'RP_COLLECTION'
    STORAGE_PATH_TRASH = 'RP_TRASH'

    OLLAMA_ENDPOINT = 'OLLAMA_ENDPOINT'
    METADATA_ENDPOINT = 'METADATA_ENDPOINT'
    EXIF_TS_ENDPOINT = 'EXIF_TS_ENDPOINT'
