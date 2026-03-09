from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from azure.storage.blob import ContainerClient, BlobClient

from rest_api_export.logging_base import Logger


@dataclass(frozen=True)
class AzureBlobArchiveConfig:
    enabled: bool = False
    overwrite: bool = True
    copy_poll_seconds: int = 2
    copy_timeout_seconds: int = 120


class AzureBlobArchiver:
    """
    Server-side copy RAW <-> ARCHIVE, radi i sa SAS container URI.

    Ulazi (credential) mogu biti:
      - str (SAS container URI)
      - dict
      - CredentialResolved (iz CredentialsStore)
    """

    def __init__(
        self,
        logger: Logger,
        source_credential: Any,
        source_container: str,
        archive_credential: Any,
        archive_container: str,
        cfg: Optional[AzureBlobArchiveConfig] = None,
    ) -> None:
        self._log = logger
        self._cfg = cfg or AzureBlobArchiveConfig()

        self._src_container = self._build_container_client(source_credential, source_container)
        self._arc_container = self._build_container_client(archive_credential, archive_container)

        self._log.info(
            "AzureBlobArchiver initialized",
            {
                "source_container": source_container,
                "archive_container": archive_container,
                "overwrite": self._cfg.overwrite,
            },
        )

    # -------------------------
    # Public API
    # -------------------------

    def send_to_archive(
        self,
        source_directory: str,
        archive_directory: str,
        filenames: List[str],
        overwrite: Optional[bool] = None,
    ) -> List[str]:
        return self._copy_many(
            direction="send_to_archive",
            from_container=self._src_container,
            to_container=self._arc_container,
            from_directory=source_directory,
            to_directory=archive_directory,
            filenames=filenames,
            overwrite=self._cfg.overwrite if overwrite is None else overwrite,
        )

    # -------------------------
    # Internals
    # -------------------------

    def _copy_many(
        self,
        direction: str,
        from_container: ContainerClient,
        to_container: ContainerClient,
        from_directory: str,
        to_directory: str,
        filenames: List[str],
        overwrite: bool,
    ) -> List[str]:
        if not filenames:
            self._log.info("Archive copy skipped: no files provided", {"direction": direction})
            return []

        from_dir = self._normalize_dir(from_directory)
        to_dir = self._normalize_dir(to_directory)

        self._log.info(
            "Archive copy started",
            {
                "direction": direction,
                "count": len(filenames),
                "from_directory": from_dir,
                "to_directory": to_dir,
                "overwrite": overwrite,
            },
        )

        created: List[str] = []

        for name in filenames:
            src_blob_name = self._join(from_dir, name)
            dst_blob_name = self._join(to_dir, name)

            src_blob = from_container.get_blob_client(src_blob_name)
            dst_blob = to_container.get_blob_client(dst_blob_name)

            if overwrite:
                self._delete_if_exists(dst_blob, direction=direction)

            src_url = self._build_source_url_with_container_sas(from_container, src_blob_name)

            self._start_copy_and_wait(
                dst_blob_client=dst_blob,
                source_url=src_url,
                direction=direction,
                src_blob_name=src_blob_name,
                dst_blob_name=dst_blob_name,
            )

            created.append(dst_blob_name)

        self._log.info(
            "Archive copy completed",
            {"direction": direction, "count": len(created), "to_container": to_container.container_name},
        )
        return created

    def _delete_if_exists(self, blob_client: BlobClient, direction: str) -> None:
        try:
            if blob_client.exists():
                self._log.info(
                    "Destination blob exists; deleting due to overwrite=true",
                    {"direction": direction, "blob": blob_client.blob_name},
                )
                blob_client.delete_blob()
        except Exception as e:
            self._log.error(
                "Failed to delete destination blob before overwrite",
                {"direction": direction, "blob": blob_client.blob_name, "error": str(e)},
            )
            raise

    def _start_copy_and_wait(
        self,
        dst_blob_client: BlobClient,
        source_url: str,
        direction: str,
        src_blob_name: str,
        dst_blob_name: str,
    ) -> None:
        try:
            self._log.info(
                "Starting server-side copy",
                {"direction": direction, "src_blob": src_blob_name, "dst_blob": dst_blob_name},
            )
            copy_props = dst_blob_client.start_copy_from_url(source_url)
            copy_id = getattr(copy_props, "copy_id", None)
            self._log.info("Copy initiated", {"direction": direction, "dst_blob": dst_blob_name, "copy_id": copy_id})
        except Exception as e:
            self._log.error(
                "Failed to start server-side copy",
                {"direction": direction, "src_blob": src_blob_name, "dst_blob": dst_blob_name, "error": str(e)},
            )
            raise

        deadline = datetime.now(timezone.utc) + timedelta(seconds=self._cfg.copy_timeout_seconds)

        import time

        while True:
            props = dst_blob_client.get_blob_properties()
            status = getattr(props.copy, "status", None) if getattr(props, "copy", None) else None

            if status in ("success", "failed", "aborted"):
                self._log.info(
                    "Copy finished",
                    {
                        "direction": direction,
                        "dst_blob": dst_blob_name,
                        "status": status,
                        "copy_id": getattr(props.copy, "id", None),
                        "copy_status_description": getattr(props.copy, "status_description", None),
                    },
                )
                if status != "success":
                    raise RuntimeError(
                        f"Server-side copy failed: status={status}, dst_blob={dst_blob_name}, "
                        f"desc={getattr(props.copy, 'status_description', None)}"
                    )
                return

            if datetime.now(timezone.utc) >= deadline:
                self._log.error("Copy timed out", {"direction": direction, "dst_blob": dst_blob_name, "last_status": status})
                raise TimeoutError(f"Copy timed out for dst_blob={dst_blob_name} (status={status})")

            time.sleep(self._cfg.copy_poll_seconds)

    # -------------------------
    # SAS URL builder
    # -------------------------

    @staticmethod
    def _build_source_url_with_container_sas(container: ContainerClient, blob_name: str) -> str:
        """
        Ako je container kreiran preko SAS URI, container.url je bez query-a,
        ali ContainerClient zna credential (sas token). Mi rekonstruišemo url:
          <container_url>/<blob_name>?<sas>
        """
        # container._config.credential je internals; zato parsiramo iz "account_url" koje je često bez SAS
        # Jednostavnije: uzmi endpoint koji nam je prosleđen kao "container_url" kada se kreira.
        # Azure SDK ne daje direktno token, pa radimo preko "from_container_url" input-a.
        # Rešenje: čuvamo sas uri kada pravimo ContainerClient (vidi _build_container_client).
        sas_uri = getattr(container, "_restapiexport_sas_uri", None)
        if not sas_uri:
            # fallback: probaj plain URL (radi samo ako je public)
            return f"{container.url.rstrip('/')}/{blob_name.lstrip('/')}"

        base, q = sas_uri.split("?", 1)
        return f"{base.rstrip('/')}/{blob_name.lstrip('/')}?{q}"

    # -------------------------
    # Container client factory
    # -------------------------

    def _build_container_client(self, credential: Any, container_name: str) -> ContainerClient:
        # podrška: str sas_uri ili dict/CredentialResolved koji imaju sas_uri
        sas_uri = self._resolve_sas_uri(credential)

        # sas_uri može biti na container (…/raw?sig=...) ili na account (…blob.core.windows.net?sig=...).
        # Ako je account-level, dodaj container_name.
        parsed = urlparse(sas_uri)
        path = parsed.path.strip("/")
        if path and "/" not in path:
            # already container path
            pass
        else:
            # account-level ili prazno -> dodaj container
            base = sas_uri.split("?", 1)[0].rstrip("/")
            q = sas_uri.split("?", 1)[1] if "?" in sas_uri else ""
            sas_uri = f"{base}/{container_name}?{q}"

        c = ContainerClient.from_container_url(sas_uri)
        setattr(c, "_restapiexport_sas_uri", sas_uri)  # stash za build_source_url_with_container_sas
        return c

    @staticmethod
    def _resolve_sas_uri(credential: Any) -> str:
        if credential is None:
            raise ValueError("credential must be provided")

        # CredentialResolved
        auth_type = getattr(credential, "auth_type", None)
        if auth_type:
            if str(auth_type).strip() != "azure_blob_sas":
                raise ValueError(f"AzureBlobArchiver expects auth_type='azure_blob_sas', got '{auth_type}'")
            sas_uri = getattr(getattr(credential, "auth", None), "sas_uri", None)
            if not sas_uri or not str(sas_uri).strip():
                raise ValueError("CredentialResolved.auth.sas_uri missing for azure_blob_sas")
            return str(sas_uri).strip()

        # dict
        if isinstance(credential, dict):
            sas_uri = credential.get("sas_uri")
            if not sas_uri and isinstance(credential.get("azure_blob_sas"), dict):
                sas_uri = credential["azure_blob_sas"].get("sas_uri")
            if not sas_uri:
                raise ValueError("azure_blob_sas dict missing sas_uri")
            return str(sas_uri).strip()

        # str
        if isinstance(credential, str):
            s = credential.strip()
            if not s:
                raise ValueError("credential string is empty")
            if s.startswith("https://") and "sig=" in s:
                return s
            raise ValueError("Unsupported credential string: expected SAS URI")

        raise ValueError("Unsupported credential type")

    # -------------------------
    # path helpers
    # -------------------------

    @staticmethod
    def _normalize_dir(d: str) -> str:
        if not d:
            return ""
        s = d.strip().strip("/")
        return f"{s}/" if s else ""

    @staticmethod
    def _join(prefix: str, name: str) -> str:
        if not prefix:
            return name.lstrip("/")
        return f"{prefix}{name.lstrip('/')}"


    # -------------------------
    # Delete directory (prefix)
    # -------------------------

    def delete_directory(
        self,
        container_type: str,
        directory: str,
    ) -> int:
        """
        Briše sve blobove iz zadatog direktorijuma (prefix).

        container_type:
            - "source"
            - "archive"

        directory:
            npr. "INTELEX_EU/CUSTOMERS"

        Vraća broj obrisanih blobova.
        """

        if container_type not in ("source", "archive"):
            raise ValueError("container_type must be 'source' or 'archive'")

        if container_type == "source":
            container=self._src_container
        else:
            container=self._arc_container


        prefix = self._normalize_dir(directory)

        self._log.info(
            "Delete directory started",
            {
                "container_type": container_type,
                "directory": prefix,
                "container": container.container_name,
            },
        )

        deleted_count = 0

        try:
            blobs = container.list_blobs(name_starts_with=prefix)


            for blob in blobs:

                blob_client = container.get_blob_client(blob.name)

                self._log.info(
                    "Deleting blob",
                    {
                        "container_type": container_type,
                        "blob": blob.name,
                    },
                )

                blob_client.delete_blob()
                deleted_count += 1

        except Exception as e:
            self._log.error(
                "Delete directory failed",
                {
                    "container_type": container_type,
                    "directory": prefix,
                    "error": str(e),
                },
            )
            raise

        self._log.info(
            "Delete directory completed",
            {
                "container_type": container_type,
                "directory": prefix,
                "deleted_count": deleted_count,
            },
        )

        return deleted_count