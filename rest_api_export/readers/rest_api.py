from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple

from rest_api_export.http_client import HttpClient
from rest_api_export.logging_base import Logger
from rest_api_export.odata_base import ODataPagedReader, ODataQuery
from rest_api_export.parsers.base import ResponseParser


@dataclass(frozen=True)
class RestApiConfig:
    """
    REST API bez paging-a: jedan HTTP poziv, parser vraća iterator redova.
    """
    pass


class RestApiReader(ODataPagedReader):
    """
    Reader za REST endpoint koji vraća XML (npr. Intelex), bez pagination-a.
    Vraća tačno jednu batch listu (ili prazno ako nema redova).
    """

    def __init__(self, http: HttpClient, parser: ResponseParser, cfg: RestApiConfig, logger: Logger):
        super().__init__(logger)
        self._http = http
        self._parser = parser
        self._cfg = cfg
        self._log.info("RestApiReader initialized")

    def iter_pages(self, url: str, query: Optional[ODataQuery] = None) -> Iterator[Tuple[List[Dict[str, Any]]], Optional[str]]:
        # Ovaj reader namerno ignoriše paging parametre (nema $top/$skip)
        if query and (query.top is not None or query.skip is not None):
            self._log.warning("RestApiReader ignores paging params", {"top": query.top, "skip": query.skip})

        headers = {"Accept": self._parser.content_type_hint()}

        self._log.info("REST API fetch (single request)", {"url": url})

        with self._http.get_stream(url, params=None, headers=headers) as resp:
            resp.raise_for_status()
            rows_iter, _next, _delta = self._parser.parse(resp)
            batch = list(rows_iter)

        if not batch:
            self._log.info("REST API finished (no rows)", {"url": url})
            return

        self._log.info("REST API finished", {"url": url, "rows": len(batch)})
        yield batch, None
