from __future__ import annotations

from typing import Optional

from rest_api_export.http_client import HttpClient, HttpConfig
from rest_api_export.logging_base import Logger
from rest_api_export.parsers.base import ResponseParser
from rest_api_export.readers.odata_v3 import ODataV3Reader, ODataV3Config
from rest_api_export.readers.rest_api import RestApiReader, RestApiConfig
from rest_api_export.readers.odata_sap import ODataSAPReader, ODataSAPConfig

from parser_factory import ParserFactory
from credentials_store import CredentialsStore  # ako ti je fajl u root-u
# ako si ga stavio u paket, onda: from rest_api_export.credentials_store import CredentialsStore


class ReaderFactory:
    def __init__(self, creds: CredentialsStore, parser_factory: ParserFactory, logger: Logger):
        self._creds = creds
        self._parsers = parser_factory
        self._log = logger

    def create(
        self,
        source_type: str,
        response_format: str,
        page_size: int,
        credential_name: str,
        instance: Optional[str] = None,
    ):
        # instance ostavljamo za kasnije (ako ti zatreba za URL logiku), auth ide preko credential_name
        parser: ResponseParser = self._parsers.create(response_format)

        cred = self._creds.get(credential_name)

        verify = cred.tls.ca_bundle if (cred.tls and cred.tls.ca_bundle) else True
        default_headers = cred.headers if cred.headers else None

        if cred.auth_type != "basic":
            raise ValueError("Unsupported auth_type for now: {0}".format(cred.auth_type))

        auth = HttpClient.basic_auth(cred.auth.username, cred.auth.password)
        http = HttpClient(auth, HttpConfig(timeout=300, verify=verify, default_headers=default_headers))

        if source_type == "odata_v3":
            return ODataV3Reader(http=http, parser=parser, cfg=ODataV3Config(page_size=page_size), logger=self._log)

        if source_type == "sap":
            # za sada koristimo isti reader paging stil
            return ODataSAPReader(http=http, parser=parser, cfg=ODataSAPConfig(), logger=self._log)

        if source_type == "rest_api":
            return RestApiReader(http=http, parser=parser, cfg=RestApiConfig(), logger=self._log)


        raise ValueError("Unknown source_type: {0}".format(source_type))
