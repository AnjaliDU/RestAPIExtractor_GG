from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple

from rest_api_export.http_client import HttpClient
from rest_api_export.logging_base import Logger
from rest_api_export.odata_base import ODataPagedReader, ODataQuery
from rest_api_export.parsers.base import ResponseParser


@dataclass(frozen=True)
class ODataSAPConfig:
    page_size: int = 2000


class ODataSAPReader(ODataPagedReader):
    """
    Generički OData v3 reader.
    Paging: klijent-driven preko $top/$skip (pošto mnogi stari servisi nemaju nextLink).
    Parser rešava XML/JSON format.
    """

    def __init__(self, http: HttpClient, parser: ResponseParser, cfg: ODataSAPConfig, logger: Logger):
        super().__init__(logger)
        self._http = http
        self._parser = parser
        self._cfg = cfg
        self._log.info("ODataV3Reader initialized", {"page_size": cfg.page_size})

    def iter_pages(
            self,
            url: str,
            query: Optional[ODataQuery] = None
    ) -> Iterator[Tuple[List[Dict[str, Any]], Optional[str]]]:

        headers = {"Accept": self._parser.content_type_hint()}

        next_url = url
        first_call = True

        while next_url:

            if first_call and query:
                params: Dict[str, str] = {}

                if query.select:
                    params["$select"] = ",".join(query.select)
                if query.filter:
                    params["$filter"] = query.filter
                if query.orderby:
                    params["$orderby"] = query.orderby

                first_call = False
            else:
                params = None  # nextLink već sadrži sve parametre

            self._log.info("OData SAP fetch", {"url": next_url})

            with self._http.get_stream(next_url, params=params, headers=headers) as resp:
                resp.raise_for_status()
                rows_iter, _next, _delta = self._parser.parse(resp)
                batch = list(rows_iter)

            if not batch:
                self._log.info("OData SAP finished (empty page)", {})
                return

            yield batch, _delta

            # 🔥 ključ: pratimo next link
            next_url = _next