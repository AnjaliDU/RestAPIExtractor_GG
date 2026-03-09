from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional


SourceType = Literal["odata_v3", "sap", "rest_api"]
ResponseFormat = Literal["atom_xml_v3", "odata_v2_json", "odata_v4_json"]


# -------------------------
# Entity (control-plane)
# -------------------------

@dataclass(frozen=True)
class EntityConfig:
    entity_id: int
    source_instance: str
    entity_name: str

    status_id: int
    running_status: int

    last_run_id: Optional[str] = None
    last_error_message: Optional[str] = None

    source_type: SourceType = "rest_api"
    pool_id: int = 1
    order_id: int = 1

    delta_enabled: int = 1
    business_keys: List[str] = None  # type: ignore

    force_snowflake: bool = False


# -------------------------
# Source
# -------------------------

@dataclass(frozen=True)
class SourceUrls:
    metadata: Optional[str] = None
    init: Optional[str] = None
    delta: Optional[str] = None


@dataclass(frozen=True)
class SourcePaging:
    next_page_attribute: Optional[str] = None
    next_page_instruction: Optional[str] = None


@dataclass(frozen=True)
class SourceDelta:
    delta_attribute: Optional[str] = None
    delta_instruction: Optional[str] = None


@dataclass(frozen=True)
class SourceSelection:
    start_date: Optional[str] = None
    filter: Optional[str] = None


@dataclass(frozen=True)
class SourceConfig:
    credential: str
    response_format: ResponseFormat

    urls: SourceUrls
    paging: SourcePaging
    delta: SourceDelta
    selection: Optional[SourceSelection] = None


# -------------------------
# Bronze / Azure landing + logs + archive
# -------------------------

@dataclass(frozen=True)
class BronzeLanding:
    storage_type: str = "azure_blob"
    connection: str = ""          # credential name, npr "AB_Data"
    container: str = ""           # npr "raw"
    directory: str = ""           # npr "INTELEX_EU/LOCATIONS"
    parquet_filename: str = ""    # npr "LOCATIONS_{audit_id}.parquet"


@dataclass(frozen=True)
class BronzeEnrichment:
    add_audit_id: bool = True
    add_entity_id: bool = True
    add_extracted_at_utc: bool = True


@dataclass(frozen=True)
class BronzeLogs:
    connection: str = ""          # credential name, npr "AB_Logs"
    container: str = ""           # npr "logs"
    directory: str = ""           # npr "INTELEX_EU"
    task_config_filename: str = ""      # npr "Execution_Tasks_{audit_id}_{guid}.json"
    execution_log_filename: str = ""    # npr "run_{audit_id}_{guid}.jsonl"


@dataclass(frozen=True)
class BronzeArchive:
    enabled: bool = False
    connection: str = ""          # credential name, npr "AB_Archive"
    container: str = ""           # npr "archive"
    directory: str = ""           # npr "INTELEX_EU/LOCATIONS"
    overwrite: bool = True
    delete_source_blob_after_archive: bool = False  # ako true: briši RAW tek kad kopija uspe


@dataclass(frozen=True)
class BronzeConfig:
    landing: BronzeLanding
    enrichment: BronzeEnrichment
    logs: BronzeLogs
    archive: Optional[BronzeArchive] = None


# -------------------------
# Snowflake
# -------------------------

@dataclass(frozen=True)
class SnowflakeDestination:
    stage_database: Optional[str] = None
    stage_schema: Optional[str] = None
    stage_name: Optional[str] = None

    file_format_database: Optional[str] = None
    file_format_schema: Optional[str] = None
    file_format_name: Optional[str] = None

    landing_database: Optional[str] = None
    landing_schema: Optional[str] = None
    landing_view_name: Optional[str] = None

    replica_database: Optional[str] = None
    replica_schema: Optional[str] = None
    replica_table_name: Optional[str] = None


@dataclass(frozen=True)
class SnowflakeScripts:
    lock_task_start: Optional[str] = None
    unlock_task_success: Optional[str] = None
    unlock_task_error: Optional[str] = None

    landing_view_create: Optional[str] = None
    replica_table_create: Optional[str] = None
    merge_apply: Optional[str] = None

    promote_status_5_to_4: Optional[str] = None


@dataclass(frozen=True)
class SnowflakeConfig:
    connection: str
    destination: SnowflakeDestination
    scripts: SnowflakeScripts


# -------------------------
# Task (execution contract)
# -------------------------

@dataclass(frozen=True)
class ExecutionTask:
    entity: EntityConfig
    source: SourceConfig
    bronze: BronzeConfig
    snowflake: SnowflakeConfig

    page_size: int = 2000
    parquet_path: str = "out/output.parquet"

    # NOVO:
    delete_local_file: bool = False
    local_archive_path: Optional[str] = None  # ako je popunjeno: move lokalni fajl ovde (posle upload-a)

    @staticmethod
    def from_dict(d: Dict[str, Any], defaults: Optional[Dict[str, Any]] = None) -> "ExecutionTask":
        merged = ExecutionTask._deep_merge_dicts(defaults or {}, d)

        # ----- entity -----
        ent = merged.get("entity") or {}
        business_keys = ent.get("business_keys") or []
        entity = EntityConfig(
            entity_id=int(ent["entity_id"]),
            source_instance=str(ent["source_instance"]),
            entity_name=str(ent["entity_name"]),
            status_id=int(ent.get("status_id", 1)),
            running_status=int(ent.get("running_status", 0)),
            last_run_id=ent.get("last_run_id"),
            last_error_message=ent.get("last_error_message"),
            source_type=ent.get("source_type", "rest_api"),
            pool_id=int(ent.get("pool_id", 1)),
            order_id=int(ent.get("order_id", 1)),
            delta_enabled=int(ent.get("delta_enabled", 1)),
            business_keys=[str(x) for x in business_keys],
            force_snowflake=bool(ent.get("force_snowflake", False)),
        )

        # ----- source -----
        src = merged.get("source") or {}
        urls = src.get("urls") or {}
        paging = src.get("paging") or {}
        delta = src.get("delta") or {}
        selection_raw = src.get("selection")

        selection = None
        if isinstance(selection_raw, dict):
            selection = SourceSelection(
                start_date=selection_raw.get("start_date"),
                filter=selection_raw.get("filter"),
            )

        source = SourceConfig(
            credential=str(src["credential"]),
            response_format=src.get("response_format", "atom_xml_v3"),
            urls=SourceUrls(
                metadata=urls.get("metadata"),
                init=urls.get("init"),
                delta=urls.get("delta"),
            ),
            paging=SourcePaging(
                next_page_attribute=paging.get("next_page_attribute"),
                next_page_instruction=paging.get("next_page_instruction"),
            ),
            delta=SourceDelta(
                delta_attribute=delta.get("delta_attribute"),
                delta_instruction=delta.get("delta_instruction"),
            ),
            selection=selection,
        )

        # ----- bronze -----
        brz = merged.get("bronze") or {}
        landing = brz.get("landing") or {}
        enrich = brz.get("enrichment") or {}
        logs = brz.get("logs") or {}
        arch = brz.get("archive") or None

        bronze_archive = None
        if isinstance(arch, dict):
            bronze_archive = BronzeArchive(
                enabled=bool(arch.get("enabled", False)),
                connection=str(arch.get("connection", "")),
                container=str(arch.get("container", "")),
                directory=str(arch.get("directory", "")),
                overwrite=bool(arch.get("overwrite", True)),
                delete_source_blob_after_archive=bool(arch.get("delete_source_blob_after_archive", False)),
            )

        bronze = BronzeConfig(
            landing=BronzeLanding(
                storage_type=str(landing.get("storage_type", "azure_blob")),
                connection=str(landing.get("connection", "")),
                container=str(landing.get("container", "")),
                directory=str(landing.get("directory", "")),
                parquet_filename=str(landing.get("parquet_filename", "")),
            ),
            enrichment=BronzeEnrichment(
                add_audit_id=bool(enrich.get("add_audit_id", True)),
                add_entity_id=bool(enrich.get("add_entity_id", True)),
                add_extracted_at_utc=bool(enrich.get("add_extracted_at_utc", True)),
            ),
            logs=BronzeLogs(
                connection=str(logs.get("connection", "")),
                container=str(logs.get("container", "")),
                directory=str(logs.get("directory", "")),
                task_config_filename=str(logs.get("task_config_filename", "")),
                execution_log_filename=str(logs.get("execution_log_filename", "")),
            ),
            archive=bronze_archive,
        )

        # ----- snowflake -----
        sn = merged.get("snowflake") or {}
        dest = sn.get("destination") or {}
        scr = sn.get("scripts") or {}

        snowflake = SnowflakeConfig(
            connection=str(sn.get("connection", "")),
            destination=SnowflakeDestination(
                stage_database=dest.get("stage_database"),
                stage_schema=dest.get("stage_schema"),
                stage_name=dest.get("stage_name"),
                file_format_database=dest.get("file_format_database"),
                file_format_schema=dest.get("file_format_schema"),
                file_format_name=dest.get("file_format_name"),
                landing_database=dest.get("landing_database"),
                landing_schema=dest.get("landing_schema"),
                landing_view_name=dest.get("landing_view_name"),
                replica_database=dest.get("replica_database"),
                replica_schema=dest.get("replica_schema"),
                replica_table_name=dest.get("replica_table_name"),
            ),
            scripts=SnowflakeScripts(
                lock_task_start=scr.get("lock_task_start"),
                unlock_task_success=scr.get("unlock_task_success"),
                unlock_task_error=scr.get("unlock_task_error"),
                landing_view_create=scr.get("landing_view_create"),
                replica_table_create=scr.get("replica_table_create"),
                merge_apply=scr.get("merge_apply"),
                promote_status_5_to_4=scr.get("promote_status_5_to_4"),
            ),
        )

        return ExecutionTask(
            entity=entity,
            source=source,
            bronze=bronze,
            snowflake=snowflake,
            page_size=int(merged.get("page_size", 2000)),
            parquet_path=str(merged.get("parquet_path", "out/output.parquet")),
            delete_local_file=bool(merged.get("delete_local_file", False)),
            local_archive_path=merged.get("local_archive_path"),
        )

    @staticmethod
    def _deep_merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        if not base:
            return dict(override)
        result: Dict[str, Any] = dict(base)
        for k, v in override.items():
            if isinstance(v, dict) and isinstance(result.get(k), dict):
                result[k] = ExecutionTask._deep_merge_dicts(result[k], v)
            else:
                result[k] = v
        return result
