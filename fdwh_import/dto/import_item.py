from dataclasses import dataclass
from datetime import datetime
from io import BytesIO

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class ImportItem:
    landing_bucket: str
    landing_bucket_key: str
    proc_datetime: datetime


@dataclass_json
@dataclass
class GoodImportItem(ImportItem):
    storage_dir: str
    storage_path: str
    status = "import"
    obj_bytes: BytesIO

    def safe_obj_bytes(self):
        self.obj_bytes.seek(0)
        return self.obj_bytes


@dataclass_json
@dataclass
class DuplicateImportItem(ImportItem):
    duplicate_bucket: str
    duplicate_bucket_key: str
    status = "duplicate"


@dataclass_json
@dataclass
class UnsupportedImportItem(ImportItem):
    unsupported_bucket: str
    unsupported_bucket_key: str
    status = "unsupported"