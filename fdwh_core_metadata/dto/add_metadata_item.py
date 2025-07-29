from dataclasses import dataclass

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class AddMetadataItem:
    abs_filename: str
    error: str | None = None
