import re
from datetime import datetime
from io import BytesIO

import requests

from fdwh_config import ImportSettings


def get_subfolder(path: str, obj_bytes: BytesIO, exif_ts_endpoint: str) -> str | None:
    sfp: str | None = get_subfolder_from_path(path)
    if sfp:
        return sfp
    else:
        return get_subfolder_from_exif(obj_bytes, exif_ts_endpoint)


def get_subfolder_from_path(path: str) -> str | None:
    s = path.split("/")
    if len(s) > 0:
        subfolder = s[0]
        if validate_subfolder_fmt(subfolder):
            return subfolder


def validate_subfolder_fmt(s: str) -> bool:
    """Validate context format: 'YYYY-MM-DD description'

    Args:
        s (str): String to validate

    Returns:
        bool: True if format is valid, False otherwise
    """
    # Check general format
    pattern = r"^(\d{4}-\d{2}-\d{2}) (.+)$"
    match = re.match(pattern, s)
    if not match:
        return False

    # Extract date for additional validation
    date_str = match.group(1)

    try:
        # Try to parse date (this will check day/month validity)
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def get_subfolder_from_exif(obj_bytes: BytesIO, exif_ts_endpoint) -> str | None:
    files = {
        'file': ('some_filename', obj_bytes, 'application/octet-stream')
    }
    response = requests.post(exif_ts_endpoint, files=files, data={'format': ImportSettings.TIMESTAMP_FMT})
    assert response.status_code == 200
    metadata = response.json()
    exif_timestamp = metadata['timestamp']
    return exif_timestamp
