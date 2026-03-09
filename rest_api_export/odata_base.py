from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple

from rest_api_export.logging_base import Logger


@dataclass(frozen=True)
class ODataQuery:
    select: Optional[List[str]] = None
    filter: Optional[str] = None
    orderby: Optional[str] = None
    top: Optional[int] = None
    skip: Optional[int] = None


class ODataPagedReader(ABC):
    def __init__(self, logger: Logger):
        self._log = logger

    @abstractmethod
    def iter_pages(self, url: str, query: Optional[ODataQuery] = None) -> Iterator[Tuple[List[Dict[str, Any]]], Optional[str]]:
        raise NotImplementedError
