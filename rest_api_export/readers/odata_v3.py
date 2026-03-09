from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

from rest_api_export.http_client import HttpClient
from rest_api_export.logging_base import Logger
from rest_api_export.odata_base import ODataPagedReader, ODataQuery
from rest_api_export.parsers.base import ResponseParser


@dataclass(frozen=True)
class ODataV3Config:
    page_size: int = 2000


class ODataV3Reader(ODataPagedReader):
    """
    Generički OData v3 reader.
    Paging: klijent-driven preko $top/$skip (pošto mnogi stari servisi nemaju nextLink).
    Parser rešava XML/JSON format.
    """

    def __init__(self, http: HttpClient, parser: ResponseParser, cfg: ODataV3Config, logger: Logger):
        super().__init__(logger)
        self._http = http
        self._parser = parser
        self._cfg = cfg
        self._log.info("ODataV3Reader initialized", {"page_size": cfg.page_size})

    def iter_pages(self, url: str, query: Optional[ODataQuery] = None) -> Iterator[List[Dict[str, Any]]]:
        page_size = query.top if query and query.top else self._cfg.page_size
        skip = query.skip if query and query.skip else 0

        headers = {"Accept": self._parser.content_type_hint()}

        while True:
            params: Dict[str, str] = {"$top": str(page_size), "$skip": str(skip)}

            if query:
                if query.select:
                    params["$select"] = ",".join(query.select)
                if query.filter:
                    params["$filter"] = query.filter
                if query.orderby:
                    params["$orderby"] = query.orderby

            self._log.info("OData v3 fetch", {"skip": skip, "top": page_size})

            with self._http.get_stream(url, params=params, headers=headers) as resp:
                resp.raise_for_status()
                rows_iter, _next = self._parser.parse(resp)
                batch = list(rows_iter)

            if not batch:
                self._log.info("OData v3 finished", {"final_skip": skip})
                return

            yield batch
            skip += page_size
