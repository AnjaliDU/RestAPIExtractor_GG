from __future__ import annotations

from typing import Any, Dict, Iterator, Optional, Tuple
from urllib.parse import urljoin

from rest_api_export.parsers.base import ResponseParser


class ODataV4JsonParser(ResponseParser):
    def content_type_hint(self) -> str:
        return "application/json"

    def parse(self, response) -> Tuple[Iterator[Dict[str, Any]], Optional[str]]:
        payload = response.json()
        rows = payload.get("value") or []
        next_link = payload.get("@odata.nextLink")
        if next_link:
            next_link = urljoin(response.url, next_link)
        return iter(rows), next_link
