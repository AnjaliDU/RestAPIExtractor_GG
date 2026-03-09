from __future__ import annotations

from typing import Any, Dict

import json
import shutil
from pathlib import Path
from typing import List, Optional, Union, Dict, Any
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

from rest_api_export.logging_base import Logger
from reader_factory import ReaderFactory
from rest_api_export.odata_base import ODataQuery
from parquet_exporter import ParquetExporter
from azure_blob_uploader import AzureBlobUploader
from azure_blob_archiver import AzureBlobArchiver, AzureBlobArchiveConfig

from task_models import ExecutionTask

from credentials_store import CredentialsStore
from snowflake_executor import (
    SnowflakeConfig,
    SnowflakeConnectionFactory,
    SnowflakeScriptExecutor,
)



class TaskExecutor:
    """
    Pure execution engine (serial).

    - writes parquet locally
    - uploads to Azure RAW (bronze.landing)
    - local post-actions: delete_local_file OR move to local_archive_path
    - optional Azure archive: RAW -> ARCHIVE (bronze.archive)
    - optional delete RAW blob after successful archive
    """

    def __init__(
        self,
        reader_factory: ReaderFactory,
        parquet: ParquetExporter,
        logger: Logger,
        azure_uploader: Optional[AzureBlobUploader] = None,
        is_load_from_config: bool = False,
    ):
        self._factory = reader_factory
        self._parquet = parquet
        self._log = logger
        self._azure = azure_uploader
        self.is_load_from_config = is_load_from_config

        self._materialized_task_path: Optional[str] = None

    def get_materialized_task_file(self) -> Optional[str]:
        return self._materialized_task_path

    # ------------------------------------------------------------------
    # Task loading
    # ------------------------------------------------------------------

    def load_tasks(self, path: str) -> List[ExecutionTask]:
        if self.is_load_from_config:
            return self.load_from_config()
        return self.load_from_json_with_audit(path)

    def load_from_config(self) -> List[ExecutionTask]:
        raise NotImplementedError("load_from_config is not implemented yet")

    def load_tasks_from_snowflake(
        self,
        query_file_path: str = "config/task_query.sql",
        credentials_path: str = "credentials.json",
        credential_name: str = "Snowflake",
        params: Optional[Dict[str, Any]] = None,
        execution_tasks_copy_path: Optional[str] = None,
    ) -> List[ExportTask]:
        """
        Učitava taskove iz Snowflake-a koristeći SQL fajl (npr. UPDATE+SELECT).
        - Kredencijale uzima iz CredentialsStore (credentials.json) pod imenom `credential_name` (default: Snowflake).
        - Vraća list[ExportTask].
        - Ako je prosleđen audit_id u params ili kroz params["audit_id"], kreira kopiju Execution_Tasks_{audit_id}.json.
        """

        self._log.info("Load tasks from Snowflake start", {
            "query_file": query_file_path,
            "credentials_file": credentials_path,
            "credential_name": credential_name,
        })

        # 1) CredentialsStore -> SnowflakeConfig
        store = CredentialsStore.load_from_file(credentials_path, logger=self._log)
        sf_cred = store.get(credential_name)
        sf_cfg = SnowflakeConfig.from_credential(sf_cred)

        # 2) Snowflake executor
        conn_factory = SnowflakeConnectionFactory(sf_cfg, logger=self._log)
        sf_exec = SnowflakeScriptExecutor(conn_factory, logger=self._log)

        # 3) Execute SQL file (multi-statement ok)
        results = sf_exec.execute_file(query_file_path, params=params)

        # 4) Uzmi poslednji result-set koji ima rows (tipično SELECT)
        select_result = next((r for r in reversed(results) if r.rows), None)
        if not select_result or not select_result.rows:
            self._log.info("No rows returned from Snowflake task query", {"query_file": query_file_path})
            return []

        # 5) Rows -> task dict -> ExportTask
        task_dicts: List[Dict[str, Any]] = []
        for row in select_result.rows:
            task_dicts.append(self._row_to_task_dict(row))

        tasks = [ExecutionTask.from_dict(d) for d in task_dicts]

        self._log.info("Load tasks from Snowflake done", {
            "task_count": len(tasks),
            "result_rows": len(select_result.rows),
        })

        # 6) Snimi kopiju Execution_Tasks_{audit_id}.json (opciono)
        audit_id = None
        if params and "audit_id" in params:
            audit_id = params.get("audit_id")

        if execution_tasks_copy_path:
            out_path = execution_tasks_copy_path
        elif audit_id is not None:
            out_path = f"Execution_Tasks_{audit_id}.json"
        else:
            out_path = None

        if out_path:
            Path(out_path).parent.mkdir(parents=True, exist_ok=True)
            Path(out_path).write_text(json.dumps(task_dicts, ensure_ascii=False, indent=2), encoding="utf-8")
            self._materialized_task_path = str(Path(out_path))
            self._log.info("Execution tasks copy written", {"path": out_path, "task_count": len(task_dicts)})
        return tasks


    @staticmethod
    def _row_to_task_dict(row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mapira red iz MONITORING.CONTROLS.REPLICATION_ENTITY_REST_API_V1 u ExecutionTask JSON shape
        koji očekuje ExecutionTask.from_dict().

        Pokriva SVE kolone iz kontrolne tabele:
        - entity: ENTITY_ID, SOURCE_INSTANCE, SOURCE_ENTITY, STATUS_ID, RUNNING_STATUS, LAST_*, SOURCE_TYPE, POOL_ID, ORDER_ID, FORCE_SNOWFLAKE, DELTA_INDICATOR, KEY_COLUNS
        - source: SOURCE_CONNECTION_DETAILS, RESPONSE_FORMAT, SOURCE_*_URL, SELECTION_JSON, NEXT_PAGE_*, DELTA_INCREMENT_*
        - bronze.landing: STAGE_*, PARQUET_FILENAME
        - bronze.enrichment: ADD_*
        - bronze.logs: LOG_*
        - bronze.archive: ARCHIVE_*
        - root: LOCAL_PARQUET_PATH, DELETE_LOCAL_FILE
        - snowflake: DESTINATION_* + scripts
        """

        # Ako query već vraća TASK_JSON/CONFIG, vrati to (opciono)
        for k in ("TASK_JSON", "TASK", "TASK_CONFIG", "CONFIG"):
            if k in row and row[k] is not None:
                v = row[k]
                if isinstance(v, dict):
                    return v
                if isinstance(v, str):
                    return json.loads(v)
                return json.loads(str(v))

        u = {str(k).upper(): v for k, v in row.items()}

        def s(val: Any) -> Optional[str]:
            if val is None:
                return None
            x = str(val).strip()
            return x if x else None

        def i(val: Any, default: int) -> int:
            try:
                return int(val)
            except Exception:
                return default

        def b(val: Any, default: bool = False) -> bool:
            if val is None:
                return default
            if isinstance(val, bool):
                return val
            return str(val).strip().lower() in ("1", "true", "t", "yes", "y")

        # -------------------------
        # entity
        # -------------------------
        entity_id = i(u.get("ENTITY_ID"), -1)
        if entity_id <= 0:
            raise ValueError(f"Missing/invalid ENTITY_ID in row: {row}")

        source_instance = s(u.get("SOURCE_INSTANCE"))
        if not source_instance:
            raise ValueError(f"Missing SOURCE_INSTANCE in row: {row}")

        entity_name = s(u.get("SOURCE_ENTITY"))
        if not entity_name:
            raise ValueError(f"Missing SOURCE_ENTITY in row: {row}")

        status_id = i(u.get("STATUS_ID"), 1)
        running_status = i(u.get("RUNNING_STATUS"), 0)

        last_run_id = s(u.get("LAST_RUN_ID"))
        last_error_message = s(u.get("LAST_ERROR_MESSAGE"))

        source_type = s(u.get("SOURCE_TYPE")) or "rest_api"
        pool_id = i(u.get("POOL_ID"), 1)
        order_id = i(u.get("ORDER_ID"), 1)
        force_snowflake = b(u.get("FORCE_SNOWFLAKE"), False)

        delta_enabled = i(u.get("DELTA_INDICATOR"), 0)

        key_cols_raw = s(u.get("KEY_COLUNS"))
        business_keys = []
        if key_cols_raw:
            sep = ";" if ";" in key_cols_raw else ","
            business_keys = [x.strip() for x in key_cols_raw.split(sep) if x.strip()]

        # -------------------------
        # source
        # -------------------------
        credential = s(u.get("SOURCE_CONNECTION_DETAILS"))
        if not credential:
            raise ValueError(f"Missing SOURCE_CONNECTION_DETAILS in row: {row}")

        response_format = s(u.get("RESPONSE_FORMAT")) or "atom_xml_v3"

        urls = {
            "metadata": s(u.get("SOURCE_METADATA_SERVICE_URL")),
            "init": s(u.get("SOURCE_URL_INIT")),
            "delta": s(u.get("SOURCE_DELTA_URL")),
        }

        paging = {
            "next_page_attribute": s(u.get("NEXT_PAGE_ATTRIBUTE")),
            "next_page_instruction": s(u.get("NEXT_PAGE_INSTRUCTION")),
        }

        delta = {
            "delta_attribute": s(u.get("DELTA_INCREMENT_ATTRIBUTE")),
            "delta_instruction": s(u.get("DELTA_INCREMENT_INSTRUCTION")),
        }

        # selection_json: VARIANT -> dict (ili string -> json)
        selection = None
        sel_val = u.get("SELECTION_JSON")
        if sel_val is not None:
            if isinstance(sel_val, dict):
                selection = sel_val
            elif isinstance(sel_val, str) and sel_val.strip():
                selection = json.loads(sel_val)
            else:
                # snowflake variant ponekad dođe kao python objekat koji nije dict; probaj str->json
                try:
                    selection = json.loads(str(sel_val))
                except Exception:
                    selection = None

        # -------------------------
        # bronze.landing
        # -------------------------
        landing = {
            "storage_type": "azure_blob",
            "connection": s(u.get("STAGE_AZURE_BLOB_CREDENTIAL")) or "",
            "container": s(u.get("STAGE_CONTAINER")) or "",
            "directory": s(u.get("STAGE_DIRECTORY")) or "",
            "parquet_filename": s(u.get("PARQUET_FILENAME")) or "",
        }

        # -------------------------
        # bronze.enrichment (iz tabele!)
        # -------------------------
        enrichment = {
            "add_audit_id": b(u.get("ADD_AUDIT_ID"), True),
            "add_entity_id": b(u.get("ADD_ENTITY_ID"), True),
            "add_extracted_at_utc": b(u.get("ADD_EXTRACTED_AT_UTC"), True),
        }

        # -------------------------
        # bronze.logs (iz tabele!)
        # -------------------------
        logs = {
            "connection": s(u.get("LOG_CONNECTION")) or "",
            "container": s(u.get("LOG_CONTAINER")) or "",
            "directory": s(u.get("LOG_DIRECTORY")) or "",
            "task_config_filename": s(u.get("LOG_TASK_CONFIG_FILENAME")) or "",
            "execution_log_filename": s(u.get("LOG_EXECUTION_FILENAME")) or "",
        }

        # -------------------------
        # bronze.archive (iz tabele!)
        # model nema poll/timeout -> ali ih možemo zadržati u mapperu kroz kolone koje postoje u modelu
        # (poll/timeout možeš kasnije dodati u model ili držati za archiver komponentu posebno)
        # -------------------------
        arch_enabled = b(u.get("ARCHIVE_ENABLED"), False)
        bronze_archive = None
        if arch_enabled or s(u.get("ARCHIVE_CONNECTION")) or s(u.get("ARCHIVE_CONTAINER")):
            bronze_archive = {
                "enabled": arch_enabled,
                "connection": s(u.get("ARCHIVE_CONNECTION")) or "",
                "container": s(u.get("ARCHIVE_CONTAINER")) or "",
                # u tabeli nema ARCHIVE_DIRECTORY -> koristi landing directory (ili dopuni kolonu ako želiš)
                "directory": s(u.get("STAGE_DIRECTORY")) or "",
                "overwrite": b(u.get("ARCHIVE_OVERWRITE"), True),
                "delete_source_blob_after_archive": b(u.get("ARCHIVE_DELETE_SOURCE_AFTER"), False),
            }

        # -------------------------
        # snowflake (destination + scripts)
        # -------------------------
        sf_connection = s(u.get("DESTINATION_CONNECTION_DETAILS")) or ""

        destination = {
            "stage_database": None,
            "stage_schema": None,
            "stage_name": None,
            "file_format_database": None,
            "file_format_schema": None,
            "file_format_name": None,
            "landing_database": s(u.get("DESTINATION_LANDING_DATABASE")),
            "landing_schema": s(u.get("DESTINATION_LANDING_SCHEMA")),
            "landing_view_name": s(u.get("DESTINATION_LANDING_TABLE")),
            "replica_database": s(u.get("DESTINATION_DATABASE")),
            "replica_schema": s(u.get("DESTINATION_SCHEMA")),
            "replica_table_name": s(u.get("DESTINATION_TABLE")),
        }

        scripts = {
            "lock_task_start": None,
            "unlock_task_success": None,
            "unlock_task_error": None,
            "landing_view_create": s(u.get("DESTINATION_LANDING_CREATE_TABLE_SCRIPT")),
            "replica_table_create": s(u.get("DESTINATION_CREATE_TABLE_SCRIPT")),
            "merge_apply": s(u.get("MERGE_APPLY_INCREMENT_SCRIPT")),
            "promote_status_5_to_4": None,
        }

        # -------------------------
        # root fields
        # -------------------------
        parquet_path = s(u.get("LOCAL_PARQUET_PATH")) or "out/output.parquet"
        delete_local_file = b(u.get("DELETE_LOCAL_FILE"), False)

        # -------------------------
        # Build final task dict
        # -------------------------
        task_dict: Dict[str, Any] = {
            "entity": {
                "entity_id": entity_id,
                "source_instance": source_instance,
                "entity_name": entity_name,
                "status_id": status_id,
                "running_status": running_status,
                "last_run_id": last_run_id,
                "last_error_message": last_error_message,
                "source_type": source_type,
                "pool_id": pool_id,
                "order_id": order_id,
                "delta_enabled": delta_enabled,
                "business_keys": business_keys,
                "force_snowflake": force_snowflake,
            },
            "source": {
                "credential": credential,
                "response_format": response_format,
                "urls": urls,
                "paging": paging,
                "delta": delta,
                "selection": selection,  # dict ili None
            },
            "bronze": {
                "landing": landing,
                "enrichment": enrichment,
                "logs": logs,
                "archive": bronze_archive,
            },
            "snowflake": {
                "connection": sf_connection,
                "destination": destination,
                "scripts": scripts,
            },
            "parquet_path": parquet_path,
            "delete_local_file": delete_local_file,
            "local_archive_path": None,
        }

        return task_dict

    def load_from_json_with_audit(self, path: str) -> List[ExecutionTask]:
        audit_id = self._log.get_audit_id()
        if audit_id is None:
            raise RuntimeError("AuditID must be set before loading tasks")

        src = Path(path)
        if not src.exists():
            raise FileNotFoundError(str(src))

        dst = src.with_name(f"Execution_Tasks_{audit_id}.json")
        dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        self._materialized_task_path = str(dst)

        self._log.info(
            "Execution task file materialized",
            {"source": str(src), "target": str(dst), "audit_id": audit_id},
        )

        return self._load_tasks_from_json(dst)

    @staticmethod
    def _load_tasks_from_json(path: Path) -> List[ExecutionTask]:
        raw: Union[List[Any], Dict[str, Any]] = json.loads(path.read_text(encoding="utf-8"))

        if isinstance(raw, list):
            return [ExecutionTask.from_dict(x) for x in raw]

        if isinstance(raw, dict):
            defaults = raw.get("defaults") or {}
            tasks = raw.get("tasks") or []
            if not isinstance(tasks, list):
                raise ValueError("Invalid task file: 'tasks' must be a list")
            return [ExecutionTask.from_dict(x, defaults=defaults) for x in tasks]

        raise ValueError("Invalid task file format")

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run_tasks(self, tasks: List[ExecutionTask]) -> None:
        self._log.info("Executor start", {"task_count": len(tasks)})

        ordered = sorted(tasks, key=lambda t: (t.entity.pool_id, t.entity.order_id, t.entity.entity_id))


        for task in ordered:

            self._run_one(task)

        self._log.info("Executor end", {"task_count": len(tasks)})

    def _run_one(self, t: ExecutionTask) -> None:
        self._log.info(
            "Task start",
            {
                "entity_id": t.entity.entity_id,
                "entity_name": t.entity.entity_name,
                "status_id": t.entity.status_id,
                "running_status": t.entity.running_status,
                "source_type": t.entity.source_type,
                "pool_id": t.entity.pool_id,
                "order_id": t.entity.order_id,
                "credential": t.source.credential,
                # važno: source_instance u context
                "source_instance": t.entity.source_instance,
            },
        )

        try:

            reader = self._factory.create(
                source_type=t.entity.source_type,
                response_format=t.source.response_format,
                page_size=t.page_size,
                credential_name=t.source.credential,
                instance=t.entity.source_instance,
            )

            base_url = t.source.urls.delta or t.source.urls.init
            if not base_url:
                raise ValueError("No source URL defined")

            final_url = self._build_final_url(base_url, t)
            query = ODataQuery(top=t.page_size)

            audit_id = self._log.get_audit_id()
            if audit_id is None:
                raise RuntimeError("AuditID must be set before executing tasks")

            audit_str = str(audit_id)
            guid = self._log.get_run_guid()

            parquet_path = (
                t.parquet_path.format(audit_id=audit_str, guid=guid)
                if t.parquet_path and ("{audit_id}" in t.parquet_path or "{guid}" in t.parquet_path)
                else t.parquet_path
            )

            Path(parquet_path).parent.mkdir(parents=True, exist_ok=True)


            # 1) Write local parquet
            delta_url=self._parquet.write_batches(
                parquet_path=parquet_path,
                batches=reader.iter_pages(final_url, query=query),
                audit_id=audit_id,
            )

            parquet_file = Path(parquet_path)

            if not parquet_file.exists():
                self._log.warning(
                    "No data returned from SAP API; parquet file not created. Skipping further processing.",
                    {
                        "entity_id": t.entity.entity_id,
                        "source_instance": t.entity.source_instance,
                        "parquet_path": parquet_path,
                        "delta_url": delta_url,
                    },
                )

            else:
                # 2) Upload RAW (bronze.landing)
                landing = t.bronze.landing
                uploaded_blob_fullname: Optional[str] = None

                if str(landing.connection or "").strip():
                    if not self._azure:
                        raise RuntimeError("Azure uploader not configured, but bronze.landing.connection is set")

                    if not str(landing.container or "").strip():
                        raise ValueError(f"bronze.landing.container missing for entity {t.entity.entity_id}")

                    if not str(landing.parquet_filename or "").strip():
                        raise ValueError(f"bronze.landing.parquet_filename missing for entity {t.entity.entity_id}")

                    blob_name = landing.parquet_filename.format(audit_id=audit_str, guid=guid)
                    blob_prefix = (landing.directory or "").strip().strip("/")

                    self._azure.upload_file(
                        local_path=parquet_path,
                        blob_name=blob_name,
                        content_type="application/octet-stream",
                        blob_prefix=blob_prefix,
                    )

                if t.snowflake and t.snowflake.scripts.merge_apply:
                    self.execute_snowflake_script(
                        command=t.snowflake.scripts.merge_apply,
                        credentials_path="credentials.json",
                        credential_name="Snowflake",
                    )
                # 3) Local post-action: MOVE or DELETE

                self._local_post_action(t, parquet_path)

                # 4) Archive (RAW -> ARCHIVE) if enabled

                self._archive_if_enabled(
                    t=t,
                    landing_container=str(landing.container or "").strip(),
                    landing_directory=str(landing.directory or "").strip().strip("/"),
                    uploaded_blob_name=(Path(uploaded_blob_fullname).name if uploaded_blob_fullname else Path(parquet_path).name),
                )

            self._log.info("Task success", {"entity_id": t.entity.entity_id, "parquet": parquet_path})
            # SUCCESS: running_status=0, last_error_message=None
            self._persist_task_state(t, running_status=0, error_message=None, delta_url=delta_url)

        except Exception as e:
            msg = str(e)
            self._log.error("Task failed", {"entity_id": t.entity.entity_id, "error": msg,
                                            "source_instance": t.entity.source_instance})

            # FAIL: running_status=2, last_error_message=msg
            self._persist_task_state(t, running_status=2, error_message=msg,delta_url=None)
            return


    # ------------------------------------------------------------------
    # Local post actions
    # ------------------------------------------------------------------

    def _local_post_action(self, t: ExecutionTask, parquet_path: str) -> None:
        p = Path(parquet_path)
        if not p.exists():
            return

        # move wins over delete
        if t.local_archive_path:
            dst = t.local_archive_path
            audit_id = self._log.get_audit_id()
            guid = self._log.get_run_guid()
            if audit_id is not None:
                dst = dst.format(audit_id=str(audit_id), guid=guid)

            dst_p = Path(dst)
            dst_p.parent.mkdir(parents=True, exist_ok=True)

            try:
                shutil.move(str(p), str(dst_p))
                self._log.info("Local parquet moved to archive", {"from": str(p), "to": str(dst_p)})
            except Exception as e:
                self._log.error("Failed to move local parquet to archive", {"from": str(p), "to": str(dst_p), "error": str(e)})
            return


        if t.delete_local_file:
            try:
                p.unlink(missing_ok=True)
                self._log.info("Local parquet deleted", {"path": str(p)})
            except Exception as e:
                self._log.error("Failed to delete local parquet", {"path": str(p), "error": str(e)})

    # ------------------------------------------------------------------
    # Archive helpers
    # ------------------------------------------------------------------

    def _archive_if_enabled(self, t: ExecutionTask, landing_container: str, landing_directory: str, uploaded_blob_name: str) -> None:
        arch = t.bronze.archive
        if not arch or not arch.enabled:
            return

        if not str(arch.connection or "").strip():
            raise ValueError(f"bronze.archive.enabled=true but bronze.archive.connection missing for entity {t.entity.entity_id}")
        if not str(arch.container or "").strip():
            raise ValueError(f"bronze.archive.enabled=true but bronze.archive.container missing for entity {t.entity.entity_id}")

        # credentials store: ReaderFactory ima creds store u sebi (kako ti već radi)
        # koristimo ._creds.get(name) jer je to tvoja postojeća integracija
        src_cred = self._factory._creds.get(str(t.bronze.landing.connection))  # type: ignore[attr-defined]
        arc_cred = self._factory._creds.get(str(arch.connection))             # type: ignore[attr-defined]

        # directory: ako archive.directory nije dat, koristi isti kao landing
        archive_dir = (arch.directory or landing_directory).strip().strip("/")

        cfg = AzureBlobArchiveConfig(enabled=True, overwrite=bool(arch.overwrite))

        archiver = AzureBlobArchiver(
            logger=self._log,
            source_credential=src_cred,
            source_container=landing_container,
            archive_credential=arc_cred,
            archive_container=str(arch.container),
            cfg=cfg,
        )

        # server-side copy
        created = archiver.send_to_archive(
            source_directory=landing_directory,
            archive_directory=archive_dir,
            filenames=[uploaded_blob_name],
            overwrite=bool(arch.overwrite),
        )

        # optional delete source RAW blob only if archive success
        if arch.delete_source_blob_after_archive and created:
            try:
                # uploader je vezan za AB_Data (raw). Treba da obrišemo baš taj blob.
                # Pošto uploader koristi container_name u upload_file, za delete koristimo SDK direktno.
                from azure.storage.blob import ContainerClient

                # build container client from same SAS as src_cred
                # reuse archiver helper by reconstructing ContainerClient
                # (najjednostavnije: opet u AzureBlobArchiver je već logika)
                tmp = AzureBlobArchiver(
                    logger=self._log,
                    source_credential=src_cred,
                    source_container=landing_container,
                    archive_credential=arc_cred,
                    archive_container=str(arch.container),
                    cfg=cfg,
                )
                src_container_client: ContainerClient = tmp._src_container  # type: ignore[attr-defined]

                src_blob_name = f"{landing_directory.strip().strip('/')}/{uploaded_blob_name}".strip("/")
                src_blob = src_container_client.get_blob_client(src_blob_name)
                if src_blob.exists():
                    src_blob.delete_blob()
                    self._log.info("Source RAW blob deleted after archive", {"blob": src_blob_name, "container": landing_container})
            except Exception as e:
                self._log.error("Failed to delete RAW blob after archive", {"error": str(e)})

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    def _build_final_url(self, base_url: str, t: ExecutionTask) -> str:
        sel = t.source.selection
        if not sel or not sel.filter:
            return base_url

        rendered = sel.filter
        if sel.start_date:
            rendered = rendered.format(start_date=sel.start_date)

        return self._append_query(base_url, rendered)

    @staticmethod
    def _append_query(url: str, query_fragment: str) -> str:
        frag = query_fragment.lstrip("?").strip()

        parsed = urlparse(url)
        existing = dict(parse_qsl(parsed.query, keep_blank_values=True))
        incoming = dict(parse_qsl(frag, keep_blank_values=True))

        merged: Dict[str, Any] = {}
        merged.update(existing)
        merged.update(incoming)

        new_query = urlencode(merged, doseq=True)

        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


    def sync_tasks_to_snowflake(
        self,
        execution_tasks_json_path: Optional[str] = None,
        credentials_path: str = "credentials.json",
        credential_name: str = "Snowflake",
        target_table: str = "MONITORING.CONTROLS.REPLICATION_ENTITY_REST_API_V1",
        only_entity_ids: Optional[List[int]] = None,
    ) -> int:
        if not execution_tasks_json_path:
            execution_tasks_json_path = self._materialized_task_path

        if not execution_tasks_json_path:
            self._log.error("sync_tasks_to_snowflake: missing execution_tasks_json_path and no materialized task file set", {})
            return 0

        self._log.info("Sync tasks to Snowflake start", {
            "path": execution_tasks_json_path,
            "credentials_file": credentials_path,
            "credential_name": credential_name,
            "target_table": target_table,
        })

        # 1) Load tasks from JSON (podržava list ili {defaults,tasks})
        tasks = self._load_tasks_from_json(Path(execution_tasks_json_path))

        if only_entity_ids:
            tasks = [t for t in tasks if t.entity.entity_id in set(only_entity_ids)]

        if not tasks:
            self._log.info("No tasks to sync", {"path": execution_tasks_json_path})
            return 0

        # 2) Snowflake connection
        store = CredentialsStore.load_from_file(credentials_path, logger=self._log)
        sf_cred = store.get(credential_name)
        sf_cfg = SnowflakeConfig.from_credential(sf_cred)

        conn_factory = SnowflakeConnectionFactory(sf_cfg, logger=self._log)
        sf_exec = SnowflakeScriptExecutor(conn_factory, logger=self._log)

        ok = 0

        # 4) Loop tasks -> params -> execute
        for t in tasks:
            try:
                params = self._task_to_control_params(t)
                sf_exec.execute_file("config/tasks_sync.sql", params=params)
                ok += 1
                self._log.info("Task synced to Snowflake", {
                    "entity_id": t.entity.entity_id,
                    "source_instance": t.entity.source_instance,
                })
            except Exception as e:
                self._log.error("Task sync failed", {
                    "entity_id": t.entity.entity_id,
                    "source_instance": t.entity.source_instance,
                    "error": str(e),
                })

        self._log.info("Sync tasks to Snowflake done", {
            "total": len(tasks),
            "synced": ok,
            "failed": len(tasks) - ok,
        })
        return ok

    def execute_snowflake_script(
            self,
            command: str,
            credentials_path: str = "credentials.json",
            credential_name: str = "Snowflake",
    ) -> None:
        """
        Izvršava jednu Snowflake SQL komandu koristeći SnowflakeScriptExecutor.execute_script().

        Parametri:
            command            -> SQL string koji se izvršava
            credentials_path   -> putanja do credentials.json
            credential_name    -> naziv Snowflake credential-a
        """

        if not command or not str(command).strip():
            raise ValueError("execute_snowflake_script: command is empty")

        self._log.info(
            "Execute Snowflake script start",
            {
                "credential_name": credential_name,
            },
        )

        # 1) Load credentials
        store = CredentialsStore.load_from_file(credentials_path, logger=self._log)
        sf_cred = store.get(credential_name)

        # 2) Build Snowflake config
        sf_cfg = SnowflakeConfig.from_credential(sf_cred)

        # 3) Connection factory + executor
        conn_factory = SnowflakeConnectionFactory(sf_cfg, logger=self._log)
        executor = SnowflakeScriptExecutor(conn_factory, logger=self._log)

        # 4) Execute SQL script (string)
        executor.execute_script(
            sql_text=str(command).strip(),
            params=None,
            stop_on_error=True,
            origin="<TaskExecutor.execute_snowflake_script>",
        )

        self._log.info(
            "Execute Snowflake script completed successfully",
            {},
        )

    def _task_to_control_params(self, t: ExecutionTask) -> Dict[str, Any]:
        """
        Mapira ExecutionTask -> param dict za UPDATE kontrolne tabele.
        Pokriva sva polja koja su definisana u TaskModels i korišćena u _row_to_task_dict docstringu.
        """

        def s(x: Any) -> Optional[str]:
            if x is None:
                return None
            v = str(x).strip()
            return v if v else None

        def b(x: Any) -> int:
            # Snowflake često drži bool kao 0/1 ili BOOLEAN; named binds će proći i bool,
            # ali da bude stabilno: vraćamo 1/0.
            if x is True:
                return 1
            if x is False:
                return 0
            return 1 if str(x).strip().lower() in ("1", "true", "t", "yes", "y") else 0

        # KEY_COLUNS u tabeli je string; u modelu business_keys je list[str]
        key_coluns = None
        if t.entity.business_keys:
            key_coluns = ",".join([x.strip() for x in t.entity.business_keys if str(x).strip()]) or None

        # selection_json: VARIANT u tabeli, ali mi šaljemo string JSON koji TRY_PARSE_JSON pojede
        selection_json = None
        if t.source.selection is not None:
            # selection je dataclass, pretvori u dict ručno
            sel = {}
            if t.source.selection.start_date is not None:
                sel["start_date"] = t.source.selection.start_date
            if t.source.selection.filter is not None:
                sel["filter"] = t.source.selection.filter
            selection_json = json.dumps(sel, ensure_ascii=False) if sel else None

        # archive
        arch = t.bronze.archive
        archive_enabled = b(arch.enabled) if arch else 0

        params: Dict[str, Any] = {
            # keys for WHERE
            "entity_id": int(t.entity.entity_id),
            "source_instance": s(t.entity.source_instance),

            # entity/state
            "status_id": int(t.entity.status_id),
            "running_status": int(t.entity.running_status),
            "last_run_id": s(t.entity.last_run_id),
            "last_error_message": s(t.entity.last_error_message),

            "source_type": s(t.entity.source_type) or "rest_api",
            "pool_id": int(t.entity.pool_id),
            "order_id": int(t.entity.order_id),
            "force_snowflake": b(t.entity.force_snowflake),
            "delta_indicator": int(t.entity.delta_enabled),
            "key_coluns": key_coluns,

            # source
            "source_connection_details": s(t.source.credential),
            "response_format": s(t.source.response_format) or "atom_xml_v3",
            "source_metadata_service_url": s(t.source.urls.metadata),
            "source_url_init": s(t.source.urls.init),
            "source_delta_url": s(t.source.urls.delta),
            "selection_json": selection_json,

            "next_page_attribute": s(t.source.paging.next_page_attribute),
            "next_page_instruction": s(t.source.paging.next_page_instruction),
            "delta_increment_attribute": s(t.source.delta.delta_attribute),
            "delta_increment_instruction": s(t.source.delta.delta_instruction),

            # bronze.landing
            "stage_azure_blob_credential": s(t.bronze.landing.connection) or "",
            "stage_container": s(t.bronze.landing.container) or "",
            "stage_directory": s(t.bronze.landing.directory) or "",
            "parquet_filename": s(t.bronze.landing.parquet_filename) or "",

            # bronze.enrichment
            "add_audit_id": b(t.bronze.enrichment.add_audit_id),
            "add_entity_id": b(t.bronze.enrichment.add_entity_id),
            "add_extracted_at_utc": b(t.bronze.enrichment.add_extracted_at_utc),

            # bronze.logs
            "log_connection": s(t.bronze.logs.connection) or "",
            "log_container": s(t.bronze.logs.container) or "",
            "log_directory": s(t.bronze.logs.directory) or "",
            "log_task_config_filename": s(t.bronze.logs.task_config_filename) or "",
            "log_execution_filename": s(t.bronze.logs.execution_log_filename) or "",

            # bronze.archive
            "archive_enabled": archive_enabled,
            "archive_connection": s(arch.connection) if arch else "",
            "archive_container": s(arch.container) if arch else "",
            "archive_overwrite": b(arch.overwrite) if arch else 1,
            "archive_delete_source_after": b(arch.delete_source_blob_after_archive) if arch else 0,

            # root/local
            "local_parquet_path": s(t.parquet_path) or "out/output.parquet",
            "delete_local_file": b(t.delete_local_file),

            # snowflake connection + destination
            "destination_connection_details": s(t.snowflake.connection) or "",
            "destination_landing_database": s(t.snowflake.destination.landing_database),
            "destination_landing_schema": s(t.snowflake.destination.landing_schema),
            "destination_landing_table": s(t.snowflake.destination.landing_view_name),
            "destination_database": s(t.snowflake.destination.replica_database),
            "destination_schema": s(t.snowflake.destination.replica_schema),
            "destination_table": s(t.snowflake.destination.replica_table_name),

            # scripts -> control table script columns
            "destination_landing_create_table_script": s(t.snowflake.scripts.landing_view_create),
            "destination_create_table_script": s(t.snowflake.scripts.replica_table_create),
            "merge_apply_increment_script": s(t.snowflake.scripts.merge_apply),
        }

        if not params["source_instance"]:
            raise ValueError("source_instance is required for sync (WHERE key)")

        return params


    def _persist_task_state(self, t: ExecutionTask, running_status: int, error_message: Optional[str], delta_url: Optional[str]) -> None:
        """
        Ažurira materialized Execution_Tasks_{audit}.json nakon izvršenja taska.

        Pravila:
        - success -> running_status=0, last_error_message=None
        - fail    -> running_status=2, last_error_message=<error>

        Napomena: ExecutionTask je frozen => update ide kroz JSON fajl, ne kroz dataclass.
        """
        path = self._materialized_task_path
        if not path:
            # Ako si taskove učitao direktno iz snowflake bez materializacije ili nije postavljeno.
            # Ne prekidamo izvršenje, samo logujemo.
            self._log.info("Materialized task file not set; skipping state persist", {
                "entity_id": t.entity.entity_id,
                "source_instance": t.entity.source_instance,
                "running_status": running_status,
                "delta_url": delta_url,
            })
            return

        p = Path(path)
        if not p.exists():
            self._log.error("Materialized task file missing; skipping state persist", {"path": str(p)})
            return

        raw = json.loads(p.read_text(encoding="utf-8"))

        # podrži format: [task,...] ili {"defaults":..., "tasks":[...]}
        if isinstance(raw, dict):
            tasks_list = raw.get("tasks") or []
        elif isinstance(raw, list):
            tasks_list = raw
        else:
            self._log.error("Invalid materialized task file format", {"path": str(p)})
            return

        updated = False

        for td in tasks_list:
            ent = (td or {}).get("entity") or {}
            if int(ent.get("entity_id", -1)) != int(t.entity.entity_id):
                continue
            if str(ent.get("source_instance", "")).strip() != str(t.entity.source_instance).strip():
                continue

            ent["running_status"] = int(running_status)
            ent["last_error_message"] = (str(error_message)[:4000] if error_message else None)
            # opciono: poslednji run id (koristi AuditID ili GUID)
            ent["last_run_id"] = str(self._log.get_run_guid())
            td["entity"] = ent

            # delta_url se čuva u source.urls (na nivou task-a), ne u entity
            if delta_url:
                td.setdefault("source", {}).setdefault("urls", {})
                td["source"]["urls"]["delta"] = str(delta_url)

            updated = True
            break

        if not updated:
            self._log.error("Task not found in materialized JSON; cannot persist state", {
                "path": str(p),
                "entity_id": t.entity.entity_id,
                "source_instance": t.entity.source_instance,
            })
            return

        # snimi nazad (isti format kao original)
        if isinstance(raw, dict):
            raw["tasks"] = tasks_list
            p.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            p.write_text(json.dumps(tasks_list, ensure_ascii=False, indent=2), encoding="utf-8")

        self._log.info("Task state persisted to materialized JSON", {
            "path": str(p),
            "entity_id": t.entity.entity_id,
            "source_instance": t.entity.source_instance,
            "running_status": running_status,
            "delta_url": delta_url,
        })