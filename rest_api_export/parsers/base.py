from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Optional, Tuple


class ResponseParser(ABC):
    """
    Parser za JEDAN HTTP response (jedna strana).
    Vraća: (iterator rows, next_link)
    next_link može biti None (tada reader radi $top/$skip).
    """

    @abstractmethod
    def content_type_hint(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def parse(self, response) -> Tuple[Iterator[Dict[str, Any]], Optional[str], Optional[str]]:
        raise NotImplementedError


