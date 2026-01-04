from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO


@dataclass
class ImportItem(ABC):
    landing_bucket: str
    landing_bucket_key: str
    proc_datetime: datetime

    @abstractmethod
    def to_dict(self) -> dict:
        pass


@dataclass
class GoodImportItem(ImportItem):
    storage_dir: str
    storage_path: str
    category = "import"
    obj_bytes: BytesIO

    def safe_obj_bytes(self):
        self.obj_bytes.seek(0)
        return self.obj_bytes

    def to_dict(self) -> dict:
        return {
            'landing_bucket': self.landing_bucket,
            'landing_bucket_key': self.landing_bucket_key,
            'category': self.category,
        }


@dataclass
class DuplicateImportItem(ImportItem):
    duplicate_bucket: str
    duplicate_bucket_key: str
    category = "duplicate"

    def to_dict(self) -> dict:
        return {
            'landing_bucket': self.landing_bucket,
            'landing_bucket_key': self.landing_bucket_key,
            'category': self.category,
            'duplicate_bucket': self.duplicate_bucket,
            'duplicate_bucket_key': self.duplicate_bucket_key
        }


@dataclass
class UnrecognizedImportItem(ImportItem):
    unrecognized_bucket: str
    unrecognized_bucket_key: str
    category = "unrecognized"
    def to_dict(self) -> dict:
        return {
            'landing_bucket': self.landing_bucket,
            'landing_bucket_key': self.landing_bucket_key,
            'category': self.category,
            'unrecognized_bucket': self.unrecognized_bucket,
            'unrecognized_bucket_key': self.unrecognized_bucket_key
        }