from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional
import hashlib
import json
import requests
from pathlib import Path

@dataclass
class RawSnapshot:
    source: str
    raw: Any
    fetched_utc: str
    content_hash: str

class BaseSource:
    def fetch_from_file(self, path: str) -> RawSnapshot:
        p = Path(path)
        text = p.read_text()
        h = hashlib.sha256(text.encode('utf-8')).hexdigest()
        return RawSnapshot(source=str(path), raw=json.loads(text), fetched_utc=str(None), content_hash=h)

    def fetch_from_url(self, url: str) -> RawSnapshot:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        text = r.text
        h = hashlib.sha256(text.encode('utf-8')).hexdigest()
        return RawSnapshot(source=url, raw=r.json(), fetched_utc=str(None), content_hash=h)
