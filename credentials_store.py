from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Union

from rest_api_export.logging_base import Logger


# =========================
# Models (resolved output)
# =========================

@dataclass(frozen=True)
class SnowflakeKeyPairResolved:
    account: str
    user: str
    private_key_path: str
    private_key_passphrase: Optional[str] = None
    role: Optional[str] = None
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    login_timeout: int = 60
    network_timeout: int = 60
    autocommit: bool = True


@dataclass(frozen=True)
class SnowflakePasswordResolved:
    account: str
    user: str
    password: str
    role: Optional[str] = None
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    login_timeout: int = 60
    network_timeout: int = 60
    autocommit: bool = True

@dataclass(frozen=True)
class TlsConfig:
    ca_bundle: Optional[str] = None


@dataclass(frozen=True)
class BasicAuthResolved:
    username: str
    password: str


@dataclass(frozen=True)
class OAuth2ClientCredentialsResolved:
    token_url: str
    client_id: str
    client_secret: str
    scope: Optional[str] = None


@dataclass(frozen=True)
class AzureBlobSasResolved:
    """
    Container-level SAS URI:
    npr. https://account.blob.core.windows.net/raw?sp=...&sig=...
    """
    sas_uri: str


@dataclass(frozen=True)
class CredentialResolved:
    """
    Jedinstven return tip za task: dobiješ auth_type + tls + headers + auth detalje.
    """
    name: str
    auth_type: str
    tls: TlsConfig
    headers: Dict[str, str]
    auth: Union[BasicAuthResolved, OAuth2ClientCredentialsResolved, AzureBlobSasResolved, SnowflakeKeyPairResolved, SnowflakePasswordResolved,]


class CredentialConfigError(ValueError):
    pass


# =========================
# Serializer / Store
# =========================

class CredentialsStore:
    """
    Store za kredencijale iz credentials.json.
    Za sada bez KeyVault-a: username/password/client_secret su direktno u fajlu.

    Kasnije: dodaćemo SecretProvider (env/keyvault) bez menjanja potrošača.
    """

    def __init__(self, credentials: Dict[str, CredentialResolved], logger: Logger):
        self._items = credentials
        self._log = logger
        self._log.info("CredentialsStore initialized", {"count": len(credentials)})

    @staticmethod
    def load_from_file(path: str, logger: Logger) -> "CredentialsStore":
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(str(p))

        data = json.loads(p.read_text(encoding="utf-8"))
        return CredentialsStore.load_from_dict(data, logger=logger)

    @staticmethod
    def load_from_dict(data: Dict[str, Any], logger: Logger) -> "CredentialsStore":
        creds = data.get("credentials")
        if not isinstance(creds, dict):
            raise CredentialConfigError("Missing or invalid 'credentials' dict in config")

        resolved: Dict[str, CredentialResolved] = {}
        for name, raw in creds.items():
            if not isinstance(raw, dict):
                raise CredentialConfigError(f"Credential '{name}' must be an object")

            item = CredentialsStore._parse_one(name=name, raw=raw)
            resolved[name] = item

        return CredentialsStore(credentials=resolved, logger=logger)

    def get(self, name: str) -> CredentialResolved:
        """
        Vrati resolved credential za ime, npr. store.get("Intelex").
        """
        if name not in self._items:
            raise KeyError(f"Unknown credential: {name}")
        self._log.info("CredentialsStore.get", {"credential": name, "auth_type": self._items[name].auth_type})
        return self._items[name]

    # -------------------------
    # Parsing helpers
    # -------------------------

    @staticmethod
    def _parse_one(name: str, raw: Dict[str, Any]) -> CredentialResolved:
        auth_type = str(raw.get("auth_type") or "").strip()
        if not auth_type:
            raise CredentialConfigError(f"Credential '{name}': missing 'auth_type'")

        tls_raw = raw.get("tls") or {}
        if tls_raw and not isinstance(tls_raw, dict):
            raise CredentialConfigError(f"Credential '{name}': 'tls' must be an object")

        tls = TlsConfig(ca_bundle=(tls_raw.get("ca_bundle") if isinstance(tls_raw, dict) else None))

        headers_raw = raw.get("headers") or {}
        if headers_raw and not isinstance(headers_raw, dict):
            raise CredentialConfigError(f"Credential '{name}': 'headers' must be an object")
        headers: Dict[str, str] = {str(k): str(v) for k, v in headers_raw.items()}

        if auth_type == "basic":
            basic = raw.get("basic")
            if not isinstance(basic, dict):
                raise CredentialConfigError(f"Credential '{name}': missing 'basic' block for auth_type=basic")

            username = str(basic.get("username") or "").strip()
            password = str(basic.get("password") or "").strip()
            if not username or not password:
                raise CredentialConfigError(f"Credential '{name}': basic.username/password are required")

            return CredentialResolved(
                name=name,
                auth_type=auth_type,
                tls=tls,
                headers=headers,
                auth=BasicAuthResolved(username=username, password=password),
            )

        if auth_type == "oauth2_client_credentials":
            o = raw.get("oauth2_client_credentials")
            if not isinstance(o, dict):
                raise CredentialConfigError(
                    f"Credential '{name}': missing 'oauth2_client_credentials' block for auth_type=oauth2_client_credentials"
                )

            token_url = str(o.get("token_url") or "").strip()
            client_id = str(o.get("client_id") or "").strip()
            client_secret = str(o.get("client_secret") or "").strip()
            scope = o.get("scope")
            scope_s = str(scope).strip() if scope is not None else None

            if not token_url or not client_id or not client_secret:
                raise CredentialConfigError(
                    f"Credential '{name}': oauth2_client_credentials.token_url/client_id/client_secret are required"
                )

            return CredentialResolved(
                name=name,
                auth_type=auth_type,
                tls=tls,
                headers=headers,
                auth=OAuth2ClientCredentialsResolved(
                    token_url=token_url,
                    client_id=client_id,
                    client_secret=client_secret,
                    scope=scope_s,
                ),
            )

        if auth_type == "azure_blob_sas":
            # Podržavamo oba formata:
            # 1) direktno na root nivou: { "sas_uri": "https://.../raw?sp=...&sig=..." }
            # 2) u bloku: { "azure_blob_sas": { "sas_uri": "..." } }
            sas_uri = str(raw.get("sas_uri") or "").strip()

            if not sas_uri:
                blk = raw.get("azure_blob_sas")
                if isinstance(blk, dict):
                    sas_uri = str(blk.get("sas_uri") or "").strip()

            if not sas_uri:
                raise CredentialConfigError(
                    f"Credential '{name}': missing 'sas_uri' for auth_type=azure_blob_sas"
                )

            return CredentialResolved(
                name=name,
                auth_type=auth_type,
                tls=tls,
                headers=headers,
                auth=AzureBlobSasResolved(sas_uri=sas_uri),
            )
        if auth_type == "snowflake_keypair":
            sf = raw.get("snowflake_keypair")
            if not isinstance(sf, dict):
                raise CredentialConfigError(f"Credential '{name}': missing 'snowflake_keypair' block for auth_type=snowflake_keypair")

            account = str(sf.get("account") or "").strip()
            user = str(sf.get("user") or "").strip()
            private_key_path = str(sf.get("private_key_path") or "").strip()
            private_key_passphrase = sf.get("private_key_passphrase")
            role = sf.get("role")
            warehouse = sf.get("warehouse")
            database = sf.get("database")
            schema = sf.get("schema")

            if not account or not user or not private_key_path:
                raise CredentialConfigError(
                    f"Credential '{name}': snowflake_keypair.account/user/private_key_path are required"
                )

            login_timeout = int(sf.get("login_timeout", 60))
            network_timeout = int(sf.get("network_timeout", 60))
            autocommit = bool(sf.get("autocommit", True))

            return CredentialResolved(
                name=name,
                auth_type=auth_type,
                tls=tls,
                headers=headers,
                auth=SnowflakeKeyPairResolved(
                    account=account,
                    user=user,
                    private_key_path=private_key_path,
                    private_key_passphrase=str(private_key_passphrase) if private_key_passphrase is not None else None,
                    role=str(role) if role is not None else None,
                    warehouse=str(warehouse) if warehouse is not None else None,
                    database=str(database) if database is not None else None,
                    schema=str(schema) if schema is not None else None,
                    login_timeout=login_timeout,
                    network_timeout=network_timeout,
                    autocommit=autocommit,
                ),
            )

        if auth_type == "snowflake_password":
            sf = raw.get("snowflake_password")
            if not isinstance(sf, dict):
                raise CredentialConfigError(f"Credential '{name}': missing 'snowflake_password' block for auth_type=snowflake_password")

            account = str(sf.get("account") or "").strip()
            user = str(sf.get("user") or "").strip()
            password = str(sf.get("password") or "").strip()
            role = sf.get("role")
            warehouse = sf.get("warehouse")
            database = sf.get("database")
            schema = sf.get("schema")

            if not account or not user or not password:
                raise CredentialConfigError(
                    f"Credential '{name}': snowflake_password.account/user/password are required"
                )

            login_timeout = int(sf.get("login_timeout", 60))
            network_timeout = int(sf.get("network_timeout", 60))
            autocommit = bool(sf.get("autocommit", True))

            return CredentialResolved(
                name=name,
                auth_type=auth_type,
                tls=tls,
                headers=headers,
                auth=SnowflakePasswordResolved(
                    account=account,
                    user=user,
                    password=password,
                    role=str(role) if role is not None else None,
                    warehouse=str(warehouse) if warehouse is not None else None,
                    database=str(database) if database is not None else None,
                    schema=str(schema) if schema is not None else None,
                    login_timeout=login_timeout,
                    network_timeout=network_timeout,
                    autocommit=autocommit,
                ),
            )


        raise CredentialConfigError(f"Credential '{name}': unsupported auth_type='{auth_type}'")
