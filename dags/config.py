dag_args_noretry = {
    'retries': 0,
}


class Conn:
    POSTGRES = 'postgres'

    SMB_ARCHIVE = "storage_smb_archive"
    SMB_COLLECTION = "storage_smb_collection"
    SMB_TRASH = "storage_smb_trash"


class AssetName:
    CORE_UPDATED = 'core_updated'


class VariableName:
    OLLAMA_ENDPOINT = 'OLLAMA_ENDPOINT'
    METADATA_ENDPOINT = 'METADATA_ENDPOINT'
    EXIF_TS_ENDPOINT = 'EXIF_TS_ENDPOINT'
