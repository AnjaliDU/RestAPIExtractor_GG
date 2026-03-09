from __future__ import annotations

from pathlib import Path
from typing import Optional, Union, Dict, Any
from urllib.parse import urlparse

from azure.storage.blob import ContainerClient, ContentSettings

from rest_api_export.logging_base import Logger

try:
    from credentials_store import CredentialResolved  # type: ignore
except Exception:  # pragma: no cover
    CredentialResolved = None  # type: ignore


class AzureBlobUploader:
    """
    Azure Blob uploader (container-scoped via SAS URI).

    Supported credential inputs:
      1) CredentialResolved (from CredentialsStore.get)
      2) dict (raw credential object)
      3) str  (SAS container URI)

    Implemented:
      - auth_type: azure_blob_sas (container-level SAS URI)

    Notes:
      - Container is fixed by SAS URI. Task 'container' is informational unless you validate it.
      - blob_name may include "directory/file.parquet".
    """

    def __init__(self, credential: Union[str, Dict[str, Any], Any], logger: Logger):
        self._log = logger
        self._container: Optional[ContainerClient] = None

        sas_uri = self._resolve_sas_uri(credential)

        # container-level SAS
        self._container = ContainerClient.from_container_url(sas_uri)

        # Extract container name for logging / validation (best-effort)
        self._container_name = self._extract_container_name_from_sas_uri(sas_uri)
        self._container_url = sas_uri.split("?")[0]

        self._log.info(
            "AzureBlobUploader initialized",
            {
                "auth_type": "azure_blob_sas",
                "container_name": self._container_name,
                "container_url": self._container_url,
            },
        )

    # -------------------------
    # Public helpers
    # -------------------------

    def get_container_name(self) -> Optional[str]:
        return self._container_name

    def validate_container(self, expected_container: str) -> None:
        """
        Optional safety: ensure task container matches SAS container.
        If we cannot extract container from SAS URI, we do not block.
        """
        exp = (expected_container or "").strip()
        if not exp:
            return

        if not self._container_name:
            # best-effort only
            self._log.info(
                "Azure container validation skipped (container name not parsed from SAS URI)",
                {"expected": exp},
            )
            return

        if self._container_name != exp:
            raise ValueError(
                f"Azure SAS container mismatch: SAS points to '{self._container_name}', task expects '{exp}'"
            )

    # -------------------------
    # Credential resolution
    # -------------------------

    def _resolve_sas_uri(self, credential: Union[str, Dict[str, Any], Any]) -> str:
        if credential is None:
            raise ValueError("credential must be provided")

        # 1) CredentialResolved instance
        if CredentialResolved is not None and isinstance(credential, CredentialResolved):
            auth_type = (credential.auth_type or "").strip()
            if auth_type != "azure_blob_sas":
                raise ValueError(f"AzureBlobUploader expects auth_type='azure_blob_sas', got '{auth_type}'")

            sas_uri = getattr(credential.auth, "sas_uri", None)
            if not sas_uri or not str(sas_uri).strip():
                raise ValueError("CredentialResolved.auth.sas_uri is missing/empty for azure_blob_sas")

            return str(sas_uri).strip()

        # 2) raw dict
        if isinstance(credential, dict):
            auth_type = str(credential.get("auth_type") or credential.get("type") or "").strip()

            if auth_type == "azure_blob_sas" or "sas_uri" in credential or "azure_blob_sas" in credential:
                sas_uri = str(credential.get("sas_uri") or "").strip()
                if not sas_uri:
                    blk = credential.get("azure_blob_sas")
                    if isinstance(blk, dict):
                        sas_uri = str(blk.get("sas_uri") or "").strip()

                if not sas_uri:
                    raise ValueError("azure_blob_sas credential dict is missing 'sas_uri'")

                return sas_uri

            raise ValueError(f"Unsupported credential dict auth_type='{auth_type}' for AzureBlobUploader")

        # 3) plain string (SAS URI)
        if isinstance(credential, str):
            s = credential.strip()
            if not s:
                raise ValueError("credential string is empty")

            if s.startswith("https://") and "sig=" in s:
                return s

            raise ValueError("Unsupported credential string: expected SAS container URI (https://...?...sig=...)")

        raise ValueError("credential must be a dict, string, or CredentialResolved")

    @staticmethod
    def _extract_container_name_from_sas_uri(sas_uri: str) -> Optional[str]:
        """
        Best-effort parse container name from:
          https://<acct>.blob.core.windows.net/<container>?...
        """
        try:
            parsed = urlparse(sas_uri)
            # path: "/raw" or "/raw/some/extra" (container-level is typically just "/raw")
            parts = [p for p in parsed.path.split("/") if p]
            return parts[0] if parts else None
        except Exception:
            return None

    # -------------------------
    # Upload
    # -------------------------

    def upload_file(
        self,
        local_path: str,
        blob_name: Optional[str] = None,
        content_type: Optional[str] = None,
        blob_prefix: Optional[str] = None,
        overwrite: bool = True,
    ) -> str:
        """
        Upload a local file to Azure Blob Storage (container fixed by SAS URI).

        Notes:
          - blob_name may already include directory ("dir/file.parquet").
          - blob_prefix is kept for backward compatibility; if provided, it prepends to blob_name.
        """
        if not self._container:
            raise RuntimeError("AzureBlobUploader is not initialized with a container client")

        p = Path(local_path)
        if not p.exists():
            raise FileNotFoundError(str(p))

        name = (blob_name or p.name).lstrip("/")

        prefix = (blob_prefix or "").strip("/")
        if prefix:
            name = f"{prefix}/{name}"

        self._log.info(
            "Azure upload start",
            {"local_path": str(p), "blob": name, "container": self._container_name},
        )

        blob_client = self._container.get_blob_client(name)
        settings = ContentSettings(content_type=content_type) if content_type else None

        with p.open("rb") as f:
            blob_client.upload_blob(f, overwrite=overwrite, content_settings=settings)

        self._log.info(
            "Azure upload success",
            {"blob": name, "container": self._container_name},
        )
        return name
