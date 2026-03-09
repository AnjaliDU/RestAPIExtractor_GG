from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union, cast

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from rest_api_export.logging_base import Logger

# Ovo importuj iz tvog modula gde je CredentialsStore
# (naziv fajla kod tebe može biti drugačiji; prilagodi import putanju)
from credentials_store import (  # type: ignore
    CredentialResolved,
    SnowflakeKeyPairResolved,
    SnowflakePasswordResolved,
)


# ============================================================
# CONFIG
# ============================================================

@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str

    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[str] = None

    password: Optional[str] = None

    role: Optional[str] = None
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None

    login_timeout: int = 900
    network_timeout: int = 3600
    autocommit: bool = True

    @staticmethod
    def from_credential(cred: CredentialResolved) -> "SnowflakeConfig":
        """
        Usklađeno sa ostatkom sistema: CredentialsStore.get("Snowflake") -> SnowflakeConfig.
        """
        if cred.auth_type == "snowflake_keypair":
            a = cast(SnowflakeKeyPairResolved, cred.auth)
            return SnowflakeConfig(
                account=a.account,
                user=a.user,
                private_key_path=a.private_key_path,
                private_key_passphrase=a.private_key_passphrase,
                role=a.role,
                warehouse=a.warehouse,
                database=a.database,
                schema=a.schema,
                login_timeout=a.login_timeout,
                network_timeout=a.network_timeout,
                autocommit=a.autocommit,
            )

        if cred.auth_type == "snowflake_password":
            a = cast(SnowflakePasswordResolved, cred.auth)
            return SnowflakeConfig(
                account=a.account,
                user=a.user,
                password=a.password,
                role=a.role,
                warehouse=a.warehouse,
                database=a.database,
                schema=a.schema,
                login_timeout=a.login_timeout,
                network_timeout=a.network_timeout,
                autocommit=a.autocommit,
            )

        raise ValueError(
            f"Credential '{cred.name}' is not a Snowflake credential (auth_type={cred.auth_type})"
        )


# ============================================================
# CONNECTION FACTORY
# ============================================================

class SnowflakeConnectionFactory:
    """
    Kreira Snowflake konekcije. Auth dolazi iz SnowflakeConfig (koji dolazi iz CredentialsStore).
    """

    def __init__(self, cfg: SnowflakeConfig, logger: Logger):
        self._cfg = cfg
        self._log = logger

    def create(self):
        import snowflake.connector  # lokalni import

        self._log.info(
            "Snowflake connect",
            {
                "account": self._cfg.account,
                "user": self._cfg.user,
                "role": self._cfg.role,
                "warehouse": self._cfg.warehouse,
                "database": self._cfg.database,
                "schema": self._cfg.schema,
                "autocommit": self._cfg.autocommit,
            },
        )

        kwargs: Dict[str, Any] = {
            "account": self._cfg.account,
            "user": self._cfg.user,
            "role": self._cfg.role,
            "warehouse": self._cfg.warehouse,
            "database": self._cfg.database,
            "schema": self._cfg.schema,
            "login_timeout": self._cfg.login_timeout,
            "network_timeout": self._cfg.network_timeout,
            "autocommit": self._cfg.autocommit,
        }

        if self._cfg.private_key_path:
            kwargs["private_key"] = self._load_private_key(
                self._cfg.private_key_path, self._cfg.private_key_passphrase
            )
        elif self._cfg.password:
            kwargs["password"] = self._cfg.password
        else:
            raise ValueError("SnowflakeConfig must have either private_key_path or password.")

        return snowflake.connector.connect(**kwargs)

    @staticmethod
    def _load_private_key(path: str, passphrase: Optional[str]):
        key_data = Path(path).read_bytes()

        private_key = serialization.load_pem_private_key(
            key_data,
            password=passphrase.encode() if passphrase else None,
            backend=default_backend(),
        )

        # Snowflake očekuje DER/PKCS8 bytes
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )


# ============================================================
# RESULT MODEL
# ============================================================

@dataclass(frozen=True)
class SnowflakeStatementResult:
    statement_index: int
    sql_preview: str
    query_id: Optional[str]
    rowcount: int
    columns: Optional[List[str]] = None
    rows: Optional[List[Dict[str, Any]]] = None


# ============================================================
# EXECUTOR (OPŠTI MULTI-STATEMENT)
# ============================================================

class SnowflakeScriptExecutor:
    """
    Opšti executor:
    - execute_file: učita SQL iz fajla i izvrši 1..N statement-a
    - execute_script: izvrši SQL string (1..N statement-a)
    - vraća listu rezultata po statement-u (SELECT vraća rows, DML/DDL nema rows)
    """

    def __init__(self, conn_factory: SnowflakeConnectionFactory, logger: Logger):
        self._factory = conn_factory
        self._log = logger

    def execute_file(
        self,
        file_path: str,
        params: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
        *,
        stop_on_error: bool = True,
    ) -> List[SnowflakeStatementResult]:
        sql = Path(file_path).read_text(encoding="utf-8")
        return self.execute_script(sql, params=params, stop_on_error=stop_on_error, origin=file_path)

    def execute_script(
        self,
        sql_text: str,
        params: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
        *,
        stop_on_error: bool = True,
        origin: str = "<inline>",
    ) -> List[SnowflakeStatementResult]:

        statements = self._split_sql_statements(sql_text)
        self._log.info("Snowflake execute_script start", {"origin": origin, "statements": len(statements)})

        results: List[SnowflakeStatementResult] = []
        conn = self._factory.create()

        try:
            with conn.cursor() as cur:
                for idx, stmt in enumerate(statements, start=1):
                    preview = self._short(stmt)
                    try:
                        if params is not None:
                            cur.execute(stmt, params)
                        else:
                            cur.execute(stmt)

                        query_id = getattr(cur, "sfqid", None)
                        rowcount = int(getattr(cur, "rowcount", -1) or -1)

                        if cur.description:
                            cols = [c[0] for c in cur.description]
                            data = cur.fetchall()
                            rows = [dict(zip(cols, r)) for r in data]
                            results.append(SnowflakeStatementResult(idx, preview, query_id, rowcount, cols, rows))
                        else:
                            results.append(SnowflakeStatementResult(idx, preview, query_id, rowcount))

                    except Exception as e:
                        self._log.error("Snowflake stmt failed", {"idx": idx, "error": str(e), "sql": preview})
                        if stop_on_error:
                            raise

            self._log.info("Snowflake execute_script done", {"origin": origin, "statements": len(statements)})
            return results

        finally:
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def _split_sql_statements(sql: str) -> List[str]:
        """
        Minimalni splitter: radi za naše standardne UPDATE; SELECT; fajlove.
        (Ako kasnije budeš imao BEGIN/END blokove sa ; unutra, proširićemo.)
        """
        parts: List[str] = []
        buffer: List[str] = []
        in_string = False

        for ch in sql:
            if ch == "'":
                in_string = not in_string
            if ch == ";" and not in_string:
                stmt = "".join(buffer).strip()
                if stmt:
                    parts.append(stmt)
                buffer = []
            else:
                buffer.append(ch)

        tail = "".join(buffer).strip()
        if tail:
            parts.append(tail)

        return parts

    @staticmethod
    def _short(sql: str, max_len: int = 180) -> str:
        s = " ".join((sql or "").split())
        return s if len(s) <= max_len else s[:max_len] + "..."
