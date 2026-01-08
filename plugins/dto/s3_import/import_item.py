from abc import ABC, abstractmethod, ABCMeta
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO


@dataclass
class ImportItem(ABC):
    landing_bucket_key: str

    @abstractmethod
    def to_dict(self) -> dict:
        pass


@dataclass
class GoodImportItem(ImportItem):
    storage_dir: str
    storage_path: str
    category = "import"
    obj_bytes: BytesIO

    def __init__(self, obj_bytes:BytesIO , subfolder: str):
        self.obj_bytes=obj_bytes
        self.landing_bucket_key=self.landing_bucket_key
        self.storage_path=subfolder + "/" + self.landing_bucket_key.split("/")[-1]
        self.storage_dir=subfolder

    def safe_obj_bytes(self):
        self.obj_bytes.seek(0)
        return self.obj_bytes

    def to_dict(self) -> dict:
        return {
            'landing_bucket_key': self.landing_bucket_key,
            'category': self.category,
        }

@dataclass
class BadImportItem(ImportItem):
    dest_key: str
    def to_dict(self) -> dict:
        pass


@dataclass
class DuplicateImportItem(BadImportItem):
    category = "duplicate"

    def to_dict(self) -> dict:
        return {
            'landing_bucket_key': self.landing_bucket_key,
            'category': self.category,
            'duplicate_bucket_key': self.dest_key
        }


@dataclass
class UnsupportedImportItem(BadImportItem):
    category = "unsupported"
    def to_dict(self) -> dict:
        return {
            'landing_bucket_key': self.landing_bucket_key,
            'category': self.category,
            'unsupported_bucket_key': self.dest_key
        }