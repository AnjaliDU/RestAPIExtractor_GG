from __future__ import annotations

from rest_api_export.logging_base import Logger
from rest_api_export.parsers.atom_xml_v3_stream import AtomXmlV3StreamParser
from rest_api_export.parsers.odata_v2_json import ODataV2JsonParser
from rest_api_export.parsers.odata_v4_json import ODataV4JsonParser
from rest_api_export.parsers.odata_sap_json import  ODataSAP
from rest_api_export.parsers.base import ResponseParser


class ParserFactory:
    def __init__(self, logger: Logger):
        self._log = logger

    def create(self, response_format: str) -> ResponseParser:
        if response_format == "atom_xml_v3":
            self._log.info("ParserFactory", {"parser": "AtomXmlV3StreamParser"})
            return AtomXmlV3StreamParser(decode_keys=True)

        if response_format == "odata_v2_json":
            self._log.info("ParserFactory", {"parser": "ODataV2JsonParser"})
            return ODataV2JsonParser()

        if response_format == "odata_v4_json":
            self._log.info("ParserFactory", {"parser": "ODataV4JsonParser"})
            return ODataV4JsonParser()

        if response_format == "odata_sap":
            self._log.info("ParserFactory", {"parser": "ODataSAP"})
            return ODataSAP()

        raise ValueError(f"Unknown response_format: {response_format}")
