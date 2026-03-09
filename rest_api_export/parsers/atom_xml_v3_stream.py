from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterator, Optional, Tuple

from rest_api_export.parsers.base import ResponseParser


class AtomXmlV3StreamParser(ResponseParser):
    NS = {
        "atom": "http://www.w3.org/2005/Atom",
        "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
        "d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
    }

    _XML_ESCAPE_RE = re.compile(r"_x([0-9A-Fa-f]{4})_")

    def __init__(self, decode_keys: bool = True):
        self._decode_keys = decode_keys

    def content_type_hint(self) -> str:
        return "*/*"

    @classmethod
    def _decode(cls, s: str) -> str:
        return cls._XML_ESCAPE_RE.sub(lambda m: chr(int(m.group(1), 16)), s)

    def parse(self, response) -> Tuple[Iterator[Dict[str, Any]], Optional[str], Optional[str]]:
        """
        Streaming iterparse.
        next_link je teško pouzdano izvući u stream modu bez dodatnog state-a,
        pa ovde vraćamo None i oslanjamo se na $top/$skip u reader-u.
        """
        stream = response.raw

        def iter_rows() -> Iterator[Dict[str, Any]]:
            context = ET.iterparse(stream, events=("end",))
            for _, elem in context:
                if elem.tag == f"{{{self.NS['atom']}}}entry":
                    content = elem.find(f"{{{self.NS['atom']}}}content")
                    if content is None:
                        elem.clear()
                        continue
                    props = content.find(f"{{{self.NS['m']}}}properties")
                    if props is None:
                        elem.clear()
                        continue

                    row: Dict[str, Any] = {}
                    for child in list(props):
                        raw_name = child.tag.split("}", 1)[-1]
                        key = self._decode(raw_name) if self._decode_keys else raw_name
                        is_null = child.attrib.get(f"{{{self.NS['m']}}}null") == "true"
                        row[key] = None if is_null else child.text

                    yield row
                    elem.clear()

        return iter_rows(), None, None
