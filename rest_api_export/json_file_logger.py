from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from rest_api_export.logging_base import Logger

from credentials_store import CredentialsStore
from snowflake_executor import (
    SnowflakeConfig,
    SnowflakeConnectionFactory,
    SnowflakeScriptExecutor,
)



class JsonFileLogger(Logger):
    """
    JSON Lines logger: svaka poruka je jedan JSON u jednoj liniji.

    Zahtevi:
      - Id brojač: startuje od 1 na init
      - Svaki zapis ima stalna polja: Id, AuditID, Guid, JobName, JobScheduler
      - context se dopunjuje (ako treba iz executora) sa source_instance
      - Errors queue: lista Id vrednosti za svaki ERROR zapis
      - get_errors(): "Errors: 3,56,432"
      - Redosled u json: Id,AuditID,Guid,JobName,JobScheduler,level,message,timestamp,context
    """

    def __init__(self, file_path_template: str):
        super().__init__()
        self._template = file_path_template

        # run meta
        self._audit_id: Optional[str] = None
        self._guid: str = str(uuid.uuid4())
        self._job_name: str = ""
        self._job_scheduler: str = ""

        # Id counter + Errors queue
        self._id_counter: int = 1
        self._errors: List[int] = []

        # trenutno aktivni path (Path objekt)
        self._path = self._resolve_path(None)
        self._path.parent.mkdir(parents=True, exist_ok=True)


    # -------------------------
    # Meta setters/getters
    # -------------------------

    def set_job_meta(self, job_name: str, job_scheduler: str) -> None:
        self._job_name = str(job_name or "")
        self._job_scheduler = str(job_scheduler or "")

    def get_run_guid(self) -> str:
        return self._guid

    def get_error_queue(self) -> List[int]:
        # kompatibilno sa postojećim main-om
        return list(self._errors)

    def get_error_message(self) -> str:
        if not self._errors:
            return "No errors"
        return "Processed with errore. See follow lines in the log: " + ",".join(str(x) for x in self._errors)


    # -------------------------
    # Path helpers
    # -------------------------

    def _resolve_path(self, audit_id: Optional[str]) -> Path:
        if "{audit_id}" in self._template:
            if audit_id:
                p = Path(self._template.format(audit_id=audit_id))
            else:
                p = Path(self._template.format(audit_id="pending"))
        else:
            p = Path(self._template)
        return p

    def get_log_path(self) -> Path:
        return self._path

    def get_audit_id(self) -> Optional[str]:
        # ako baza Logger već ima get_audit_id, preferiraj je
        try:
            v = super().get_audit_id()  # type: ignore[attr-defined]
            if v is not None:
                self._audit_id = str(v)
        except Exception:
            pass
        return self._audit_id

    def set_audit_id(self, audit_id: Optional[str]) -> None:
        """
        Promeni internu putanju fajla tako da sadrzi audit_id.
        """
        self._audit_id = str(audit_id) if audit_id is not None else None

        new_path = self._resolve_path(self._audit_id)
        if new_path != self._path:
            new_path.parent.mkdir(parents=True, exist_ok=True)
            self._path = new_path

            # ovo je “normalan” log zapis (sa Id itd.)
            self.info(
                "Logger path switched to include AuditID",
                {"path": str(self._path), "audit_id": self._audit_id},
            )

    # -------------------------
    # Core logging override
    # -------------------------

    def log(self, level: str, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Override base Logger.log da ubaci:
          - Id counter
          - AuditID/Guid/Job meta
          - Errors queue
          - redosled polja u JSON-u
        """
        current_id = self._id_counter
        self._id_counter += 1

        if str(level).upper() == "ERROR":
            self._errors.append(current_id)

        ts = datetime.utcnow().isoformat()

        entry: Dict[str, Any] = {
            "Id": current_id,
            "AuditID": self.get_audit_id(),
            "Guid": self._guid,
            "JobName": self._job_name,
            "JobScheduler": self._job_scheduler,
            "level": str(level).upper(),
            "message": message,
            "timestamp": ts,
            "context": context or {},
        }

        self._write(entry)

    # -------------------------
    # Writer
    # -------------------------

    def _write(self, entry: Dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as f:
            json.dump(entry, f, ensure_ascii=False)
            f.write("\n")

    def save_to_db(self, entry: Dict[str, Any]) -> None:
        return

    # -------------------------
    # Snowflake placeholders
    # -------------------------

    def start_job(self, query_file_path="config/start_job.sql",
        credentials_path="credentials.json",
        credential_name="Snowflake", context=None) -> int:

        context = context or {}

        # 2) Snowflake connection
        store = CredentialsStore.load_from_file(credentials_path, logger=self)
        sf_cred = store.get(credential_name)
        sf_cfg = SnowflakeConfig.from_credential(sf_cred)

        conn_factory = SnowflakeConnectionFactory(sf_cfg, logger=self)
        sf_exec = SnowflakeScriptExecutor(conn_factory, logger=self)

        params = {
            "_job_name": self._job_name,
            "_job_scheduler": self._job_scheduler,
            "_run_id": self._guid,  # koristimo GUID kao run id
        }

        results = sf_exec.execute_file(
            query_file_path,
            params=params
        )

        if not results:
            raise RuntimeError("START_JOB did not return any result.")

        # Uzmi prvi resultset koji ima rows
        for r in reversed(results):
            if r.rows:
                row = r.rows[0]
                audit_id = list(row.values())[0]  # ili row["AUDIT_ID"] ako postoji
                break
        else:
            raise RuntimeError("START_JOB did not return AUDIT_ID.")

        audit_id = int(audit_id)

        self.set_audit_id(str(audit_id))
        self.info("Job started (snowflake)", {"audit_id": audit_id, **context})

        return audit_id

    def finish_job(self, success: bool, query_file_path="config/finish_job.sql",
        credentials_path="credentials.json",
        credential_name="Snowflake", context=None) -> None:
        context = context or {}

        audit_id = self.get_audit_id()
        if not audit_id:
            raise RuntimeError("Audit ID not set before finish_job.")


        store = CredentialsStore.load_from_file(credentials_path, logger=self)
        sf_cred = store.get(credential_name)
        sf_cfg = SnowflakeConfig.from_credential(sf_cred)

        conn_factory = SnowflakeConnectionFactory(sf_cfg, logger=self)
        sf_exec = SnowflakeScriptExecutor(conn_factory, logger=self)

        status_id = 1 if success and not self.get_error_queue() else 2
        error_message = self.get_error_message() or None

        params = {
            "_audit_id": int(audit_id),
            "status_id": status_id,
            "error_message": error_message,
        }

        sf_exec.execute_file(
            query_file_path,
            params=params
        )

        self.info(
            "Job finished (snowflake)",
            {
                "audit_id": audit_id,
                "status_id": status_id,
                **context
            }
        )

    # ------------------------------------------------------------------
    # Azure upload + cleanup (POZIVA SE IZ MAIN KAO POSLEDNJA KOMANDA)
    # ------------------------------------------------------------------

    def upload_and_cleanup_logs(
        self,
        uploader: Any,
        logs_directory: str,
        execution_log_filename: str,
        materialized_task_local_path: Optional[str],
        task_config_filename: Optional[str],
        delete_local_files: bool,
    ) -> None:
        """
        Uploaduje na Azure:
          - execution log (lokalni jsonl)
          - cloned task config (Execution_Tasks_*.json) ako postoji

        i po potrebi briše lokalno:
          - prvo cloned task config
          - na kraju log fajl

        VAŽNO:
          Ovu metodu pozovi kao POSLEDNJU u main-u.
          NEMA logovanja posle brisanja log fajla.
        """
        audit_id = self.get_audit_id()
        if audit_id is None:
            raise RuntimeError("AuditID missing in upload_and_cleanup_logs")

        guid = self.get_run_guid()
        audit_str = str(audit_id)

        base_dir = (logs_directory or "").strip().strip("/")

        local_log = str(self.get_log_path())

        # 1) upload execution log
        exec_name = (
            execution_log_filename.format(audit_id=audit_str, guid=guid)
            if execution_log_filename
            else Path(local_log).name
        )
        exec_blob = f"{base_dir}/{exec_name}" if base_dir else exec_name

        self.info("Logs upload start", {"local_path": local_log, "blob": exec_blob})
        uploader.upload_file(
            local_path=local_log,
            blob_name=exec_blob,
            content_type="application/json",
            overwrite=True,
        )
        self.info("Logs upload success", {"blob": exec_blob})

        # 2) upload cloned task config (ako postoji)
        if materialized_task_local_path and task_config_filename:
            tc_name = task_config_filename.format(audit_id=audit_str, guid=guid)
            tc_blob = f"{base_dir}/{tc_name}" if base_dir else tc_name

            self.info("Task config upload start", {"local_path": materialized_task_local_path, "blob": tc_blob})
            uploader.upload_file(
                local_path=materialized_task_local_path,
                blob_name=tc_blob,
                content_type="application/json",
                overwrite=True,
            )
            self.info("Task config upload success", {"blob": tc_blob})

            if delete_local_files:
                try:
                    Path(materialized_task_local_path).unlink(missing_ok=True)
                    self.info("Local task config deleted", {"path": materialized_task_local_path})
                except Exception as e:
                    self.error("Failed to delete local task config", {"path": materialized_task_local_path, "error": str(e)})

        # 3) delete local log as LAST (bez logovanja posle!)
        if delete_local_files:
            try:
                Path(local_log).unlink(missing_ok=True)
            except Exception:
                pass
