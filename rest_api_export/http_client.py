from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import requests
from requests.auth import AuthBase, HTTPBasicAuth


@dataclass(frozen=True)
class HttpConfig:
    timeout: int = 60
    verify: str | bool = True
    default_headers: Optional[Dict[str, str]] = None


class HttpClient:
    """
    Transport sloj: session + auth + tls verify + stream GET.
    Nema OData logike.
    """

    def __init__(self, auth: AuthBase, config: HttpConfig):
        self._session = requests.Session()
        self._session.auth = auth
        self._config = config
        if config.default_headers:
            self._session.headers.update(config.default_headers)

    @staticmethod
    def basic_auth(username: str, password: str) -> AuthBase:
        return HTTPBasicAuth(username, password)

    def get_stream(self, url: str, params: Optional[Dict[str, str]] = None, headers: Optional[Dict[str, str]] = None):
        resp = self._session.get(
            url,
            params=params,
            headers=headers,
            stream=True,
            timeout=self._config.timeout,
            verify=self._config.verify,
        )
        resp.raw.decode_content = True
        return resp
