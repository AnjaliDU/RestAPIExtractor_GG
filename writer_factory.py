from __future__ import annotations

from typing import Dict

from rest_api_export.logging_base import Logger
from credentials_store import CredentialsStore
from azure_blob_uploader import AzureBlobUploader


class WriterFactory:
    """
    Factory za 'writer' komponente (sinks), npr:
      - azure_blob uploader (bronze landing / logs)
      - kasnije: snowflake writer, filesystem writer, itd.

    Kreira i кешira instance po connection imenu iz task-a (npr. "AB_Data").
    """

    def __init__(self, store: CredentialsStore, logger: Logger):
        self._store = store
        self._log = logger
        self._azure_cache: Dict[str, AzureBlobUploader] = {}

        self._log.info("WriterFactory initialized", {})

    def get_azure_uploader(self, connection_name: str) -> AzureBlobUploader:
        name = (connection_name or "").strip()
        if not name:
            raise ValueError("Azure connection_name is empty")

        if name in self._azure_cache:
            return self._azure_cache[name]

        cred = self._store.get(name)  # CredentialResolved
        uploader = AzureBlobUploader(cred, self._log)

        self._azure_cache[name] = uploader
        self._log.info("Azure uploader created (cached)", {"connection": name})
        return uploader
