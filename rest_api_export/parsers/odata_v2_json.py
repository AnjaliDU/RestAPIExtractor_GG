from __future__ import annotations

from typing import Any, Dict, Iterator, Optional, Tuple

from rest_api_export.parsers.base import ResponseParser


class ODataV2JsonParser(ResponseParser):
    def content_type_hint(self) -> str:
        return "application/json"

    def parse(self, response) -> Tuple[Iterator[Dict[str, Any]], Optional[str]]:
        payload = response.json()
        d = payload.get("d") or {}
        rows = d.get("results") or []
        next_url = d.get("__next")
        return iter(rows), next_url
