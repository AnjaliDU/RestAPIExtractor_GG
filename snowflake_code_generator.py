from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from urllib.parse import urlparse

import requests
import pyarrow.parquet as pq
import xml.etree.ElementTree as ET

# ==========================================================
# EDM → Snowflake type mapping
# ==========================================================

EDM_TO_SNOWFLAKE = {
    "Edm.String": "VARCHAR",
    "Edm.Int32": "NUMBER(38,0)",
    "Edm.Int64": "NUMBER(38,0)",
    "Edm.Decimal": "NUMBER(38,10)",
    "Edm.Double": "FLOAT",
    "Edm.Single": "FLOAT",
    "Edm.Boolean": "BOOLEAN",
    "Edm.DateTime": "TIMESTAMP_NTZ",
}


class ColumnSanitizer:

    @staticmethod
    def sanitize(name: str) -> str:
        # ukloni _x0020_
        name = name.replace("_x0020_", "_")

        # ukloni "-"
        name = name.replace("-", "_")

        # zadrži samo A-Za-z0-9_
        name = re.sub(r"[^A-Za-z0-9_]", "", name)

        name = re.sub(r"_+", "_", name)
        return name


# ==========================================================
# Credentials Store
# ==========================================================

class CredentialsStore:

    def __init__(self, credentials_path: Path) -> None:
        with credentials_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        self._store = data["credentials"] if "credentials" in data else data

    def get(self, name: str) -> Dict[str, Any]:
        if name not in self._store:
            raise KeyError(f"Credential '{name}' not found")
        return self._store[name]

    def get_basic_auth(self, name: str) -> Tuple[str, str, Dict[str, str]]:
        cred = self.get(name)
        return (
            cred["basic"]["username"],
            cred["basic"]["password"],
            cred.get("headers", {}),
        )

    def get_blob_sas(self, name: str) -> Tuple[str, str]:
        cred = self.get(name)
        sas_uri = cred["sas_uri"]
        parsed = urlparse(sas_uri)

        storage_account = parsed.netloc.split(".")[0]
        sas_token = parsed.query

        return storage_account, sas_token


# ==========================================================
# OData URL Builder
# ==========================================================

class ODataUrlBuilder:

    @staticmethod
    def build_metadata_url(data_url: str) -> str:
        if data_url.endswith("/Data"):
            return data_url[:-5] + "/$metadata"
        raise ValueError(f"Cannot derive metadata URL from: {data_url}")


# ==========================================================
# Metadata Service
# ==========================================================

class MetadataService:

    def __init__(self, credentials_store: CredentialsStore) -> None:
        self._credentials_store = credentials_store

    def fetch(
        self,
        credential_name: str,
        metadata_url: str
    ) -> Optional[str]:

        try:
            username, password, headers = \
                self._credentials_store.get_basic_auth(credential_name)

            response = requests.get(
                metadata_url,
                auth=(username, password),
                headers=headers,
                timeout=60,
            )
            response.raise_for_status()
            return response.text

        except Exception:
            return None

    @staticmethod
    def parse(edmx_xml: str) -> Dict[str, Tuple[str, bool]]:
        ns = {"edm": "http://schemas.microsoft.com/ado/2008/09/edm"}
        root = ET.fromstring(edmx_xml)

        result: Dict[str, Tuple[str, bool]] = {}

        for entity in root.findall(".//edm:EntityType[@Name='Data']", ns):
            for prop in entity.findall("edm:Property", ns):
                name = prop.attrib["Name"]
                typ = prop.attrib["Type"]
                nullable = prop.attrib.get("Nullable", "true").lower() == "true"
                result[name] = (typ, nullable)

        return result


# ==========================================================
# Parquet Reader
# ==========================================================

class ParquetSchemaReader:

    @staticmethod
    def resolve_path(template: str, audit_id: int) -> Path:
        return Path(template.replace("{audit_id}", str(audit_id)))

    @staticmethod
    def read_columns(parquet_path: Path) -> List[str]:
        pf = pq.ParquetFile(parquet_path)
        return [field.name for field in pf.schema_arrow]


# ==========================================================
# Snowflake DDL Generator
# ==========================================================

class SnowflakeDdlGenerator:

    # ---------------- VIEW CAST ----------------

    def _view_expression(
        self,
        column: str,
        metadata_map: Optional[Dict[str, Tuple[str, bool]]],
    ) -> str:

        base = f'$1:"{column}"::VARCHAR'

        if not metadata_map or column not in metadata_map:
            return base

        edm_type, _ = metadata_map[column]

        if edm_type in ("Edm.Int32", "Edm.Int64"):
            return f'TRY_TO_NUMBER({base})::NUMBER(38,0)'

        if edm_type == "Edm.Decimal":
            return f'TRY_TO_DECIMAL({base}, 38, 10)'

        if edm_type in ("Edm.Single", "Edm.Double"):
            return f'TRY_TO_DOUBLE({base})'

        if edm_type == "Edm.Boolean":
            return f'TRY_TO_BOOLEAN({base})'

        if edm_type == "Edm.DateTime":
            return f'TRY_TO_TIMESTAMP_NTZ({base})'

        return base

    # ---------------- MERGE APPLY ----------------

    def _generate_merge_apply(
            self,
            task: Dict[str, Any],
            parquet_columns: List[str],
    ) -> str:

        destination = task["snowflake"]["destination"]
        business_keys = task["entity"].get("business_keys", [])

        parquet_columns = [c for c in parquet_columns if str(c).lower() != "__metadata"]

        landing_db = ColumnSanitizer.sanitize(destination["landing_database"])
        landing_schema = ColumnSanitizer.sanitize(destination["landing_schema"])
        view_name = ColumnSanitizer.sanitize(destination["landing_view_name"])

        replica_db = ColumnSanitizer.sanitize(destination["replica_database"])
        replica_schema = ColumnSanitizer.sanitize(destination["replica_schema"])
        table_name = ColumnSanitizer.sanitize(destination["replica_table_name"])

        full_view = f"{landing_db}.{landing_schema}.{view_name}"
        full_table = f"{replica_db}.{replica_schema}.{table_name}"

        # FULL RELOAD
        if not business_keys:
            return f"""
    CREATE OR REPLACE TABLE {full_table} AS
    SELECT *
    FROM {full_view};
    """.strip()

        # --- NEW: dedup + change mode config ---
        # ---------------------------------------------------------
        # Dedup config (bez menjanja TaskModel.py)
        # ---------------------------------------------------------

        dedup_cfg = (task.get("entity") or {}).get("dedup") or {}

        # default SAP kolona za change mode
        change_col = dedup_cfg.get("change_mode_column") or "ODQ_CHANGEMODE"

        # default delete vrednost
        delete_val = dedup_cfg.get("delete_value") or "D"

        # order_by može biti:
        #   - lista ["COL1", "COL2"]
        #   - string "COL1 DESC"
        #   - None
        order_by_cfg = dedup_cfg.get("order_by")

        if order_by_cfg is None:
            # default SAP fallback
            extra_order_cols = ["ODQ_ENTITYCNTR"]
        elif isinstance(order_by_cfg, str):
            extra_order_cols = [order_by_cfg]
        elif isinstance(order_by_cfg, list):
            extra_order_cols = order_by_cfg
        else:
            raise ValueError("Invalid type for dedup.order_by")

        # validacija: AUDIT_ID mora postojati za dedup across files
        if "AUDIT_ID" not in parquet_columns:
            raise ValueError("MERGE requires AUDIT_ID column for dedup/order_by")

        # ROW_NUMBER ORDER BY: AUDIT_ID DESC, <extra cols> DESC
        order_by_sql = ['"AUDIT_ID" DESC']
        for c in extra_order_cols:
            c2 = ColumnSanitizer.sanitize(str(c))
            order_by_sql.append(f'"{c2}" DESC')

        partition_by_sql = ", ".join([f'"{col}"' for col in business_keys])

        using_sql = f"""
    (
      SELECT * FROM (
        SELECT
          V.*,
          ROW_NUMBER() OVER (
            PARTITION BY {partition_by_sql}
            ORDER BY {", ".join(order_by_sql)}
          ) AS _RN
        FROM {full_view} V
      )
      WHERE _RN = 1
    ) S
    """.strip()

        join_condition = " AND ".join([f'T."{col}" = S."{col}"' for col in business_keys])

        # update set: obično ne želiš da menjaš business keys, ali možeš ako hoćeš
        update_set = ",\n".join([f'    T."{col}" = S."{col}"' for col in parquet_columns])

        insert_columns = ", ".join([f'"{c}"' for c in parquet_columns])
        insert_values = ", ".join([f'S."{c}"' for c in parquet_columns])

        # Change-mode predicates
        # - delete: WHEN MATCHED AND S.<change_col> = 'D' THEN DELETE
        # - insert: WHEN NOT MATCHED AND (S.<change_col> IS NULL OR S.<change_col> <> 'D') THEN INSERT
        delete_clause = ""
        insert_predicate = ""

        if change_col:
            change_col = ColumnSanitizer.sanitize(str(change_col))
            delete_clause = f'\nWHEN MATCHED AND S."{change_col}" = \'{delete_val}\' THEN DELETE'
            insert_predicate = f' AND (S."{change_col}" IS NULL OR S."{change_col}" <> \'{delete_val}\')'

        return f"""
    MERGE INTO {full_table} T
    USING {using_sql}
    ON {join_condition}
    {delete_clause}

    WHEN MATCHED THEN UPDATE SET
    {update_set}

    WHEN NOT MATCHED{insert_predicate} THEN INSERT
    ({insert_columns})
    VALUES
    ({insert_values});
    """.strip()
    # ---------------- MAIN GENERATE ----------------

    def generate(
            self,
            task: Dict[str, Any],
            parquet_columns: List[str],
            metadata_map: Optional[Dict[str, Tuple[str, bool]]],
            storage_account: str,
            sas_token: str,
    ) -> Dict[str, str]:

            destination = task["snowflake"]["destination"]
            bronze = task["bronze"]

            # izbaci __metadata globalno (za view/table/merge)
            parquet_columns = [c for c in parquet_columns if str(c).lower() != "__metadata"]

            landing_db = ColumnSanitizer.sanitize(destination["landing_database"])
            landing_schema = ColumnSanitizer.sanitize(destination["landing_schema"])
            view_name = ColumnSanitizer.sanitize(destination["landing_view_name"])

            replica_db = ColumnSanitizer.sanitize(destination["replica_database"])
            replica_schema = ColumnSanitizer.sanitize(destination["replica_schema"])
            table_name = ColumnSanitizer.sanitize(destination["replica_table_name"])

            stage_name = ColumnSanitizer.sanitize(f"{view_name}_STG")
            file_format_name = ColumnSanitizer.sanitize(f"{view_name}_PARQUET_FMT")

            container = bronze["landing"]["container"]
            directory = bronze["landing"]["directory"]

            file_format_sql = f"""
    CREATE FILE FORMAT IF NOT EXISTS {landing_db}.{landing_schema}.{file_format_name}
    TYPE = PARQUET
    COMPRESSION = AUTO;
    """.strip()

            stage_sql = f"""
    CREATE STAGE IF NOT EXISTS {landing_db}.{landing_schema}.{stage_name}
    URL = 'azure://{storage_account}.blob.core.windows.net/{container}/{directory}'
    CREDENTIALS = (AZURE_SAS_TOKEN = '{sas_token}')
    FILE_FORMAT = {landing_db}.{landing_schema}.{file_format_name};
    """.strip()

            view_columns_sql = ",\n".join(
                [f'    {self._view_expression(col, metadata_map)} AS "{col}"' for col in parquet_columns]
            )

            view_sql = f"""
    CREATE OR REPLACE VIEW {landing_db}.{landing_schema}.{view_name} AS
    SELECT
    {view_columns_sql}
    FROM @{landing_db}.{landing_schema}.{stage_name};
    """.strip()

            table_columns: List[str] = []
            for col in parquet_columns:
                if metadata_map and col in metadata_map:
                    edm_type, nullable = metadata_map[col]
                    snow_type = EDM_TO_SNOWFLAKE.get(edm_type, "VARCHAR")
                    null_sql = "" if nullable else " NOT NULL"
                else:
                    snow_type = "VARCHAR"
                    null_sql = ""

                table_columns.append(f'    "{col}" {snow_type}{null_sql}')

            # PRIMARY KEY (ako postoje business_keys)
            business_keys = task.get("entity", {}).get("business_keys", []) or []
            bk = [ColumnSanitizer.sanitize(c) for c in business_keys]
            pk_sql = ""
            if bk:
                # constraint name čisto informativan
                pk_name = ColumnSanitizer.sanitize(f"PK_{table_name}")
                pk_cols = ", ".join([f'"{c}"' for c in bk])
                pk_sql = f",\n    CONSTRAINT {pk_name} PRIMARY KEY ({pk_cols})"

            table_sql = f"""
    CREATE TABLE IF NOT EXISTS {replica_db}.{replica_schema}.{table_name}
    (
    {",\n".join(table_columns)}{pk_sql}
    );
    """.strip()

            merge_sql = self._generate_merge_apply(task, parquet_columns)

            return {
                "landing_view_create": f"{file_format_sql}\n\n{stage_sql}\n\n{view_sql}",
                "replica_table_create": table_sql,
                "merge_apply": merge_sql,
            }
# ==========================================================
# Main Orchestrator
# ==========================================================

class SnowflakeCodeGenerator:

    def __init__(
        self,
        audit_id: int,
        tasks_path: Path,
        credentials_path: Path,
    ) -> None:
        self._audit_id = audit_id
        self._tasks_path = tasks_path
        self._credentials = CredentialsStore(credentials_path)
        self._metadata_service = MetadataService(self._credentials)
        self._parquet_reader = ParquetSchemaReader()
        self._ddl_generator = SnowflakeDdlGenerator()

    def run(self) -> None:

        with self._tasks_path.open("r", encoding="utf-8") as f:
            tasks = json.load(f)

        for task in tasks:

            parquet_template = task["parquet_path"]
            parquet_path = self._parquet_reader.resolve_path(
                parquet_template,
                self._audit_id,
            )

            parquet_columns = self._parquet_reader.read_columns(parquet_path)

            #data_url = task["source"]["urls"]["init"]
            metadata_url = task["source"]["urls"]["metadata"] #ODataUrlBuilder.build_metadata_url(data_url)

            metadata_xml = self._metadata_service.fetch(
                task["source"]["credential"],
                metadata_url,
            )

            metadata_map = (
                self._metadata_service.parse(metadata_xml)
                if metadata_xml
                else None
            )

            blob_connection = task["bronze"]["landing"]["connection"]
            storage_account, sas_token = \
                self._credentials.get_blob_sas(blob_connection)

            ddl = self._ddl_generator.generate(
                task,
                parquet_columns,
                metadata_map,
                storage_account,
                sas_token,
            )

            scripts = task["snowflake"]["scripts"]
            scripts["landing_view_create"] = ddl["landing_view_create"]
            scripts["replica_table_create"] = ddl["replica_table_create"]
            scripts["merge_apply"] = ddl["merge_apply"]

        with self._tasks_path.open("w", encoding="utf-8") as f:
            json.dump(tasks, f, indent=2, ensure_ascii=False)


# ==========================================================
# MAIN
# ==========================================================

def main() -> None:

    audit_id = 356

    base_path = Path(__file__).parent / ""

    tasks_path = base_path / "Execution_Tasks_356.json"
    credentials_path = base_path / "credentials.json"

    generator = SnowflakeCodeGenerator(
        audit_id=audit_id,
        tasks_path=tasks_path,
        credentials_path=credentials_path,
    )

    generator.run()


if __name__ == "__main__":
    main()