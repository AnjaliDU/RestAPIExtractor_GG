from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4


@dataclass(frozen=True)
class JobMeta:
    job_name: str = ""
    job_scheduler: str = ""


class Logger(ABC):
    """
    Logger interfejs + zajednička meta polja:
      - AuditID (setuje se spolja)
      - Guid (run guid, uvek nov po pokretanju)
      - JobName / JobScheduler (setuje se u main)
      - ErrorQueue (lista Id vrednosti za ERROR zapise)
    """

    def __init__(self) -> None:
        self._audit_id: Optional[str] = None
        self._run_guid: str = str(uuid4())
        self._job_meta: JobMeta = JobMeta()
        self._error_queue: List[int] = []

    # -------------------------
    # Meta getters/setters
    # -------------------------

    def set_audit_id(self, audit_id: Optional[str]) -> None:
        self._audit_id = str(audit_id) if audit_id is not None else None

    def get_audit_id(self) -> Optional[str]:
        return self._audit_id

    def get_run_guid(self) -> str:
        return self._run_guid

    def set_job_meta(self, job_name: str, job_scheduler: str) -> None:
        self._job_meta = JobMeta(job_name=str(job_name or ""), job_scheduler=str(job_scheduler or ""))

    def get_job_meta(self) -> JobMeta:
        return self._job_meta

    def get_error_queue(self) -> List[int]:
        # vraćamo kopiju da niko ne mutira spolja
        return list(self._error_queue)

    def _push_error_id(self, log_id: int) -> None:
        self._error_queue.append(int(log_id))

    # -------------------------
    # Logging API
    # -------------------------

    def log(self, level: str, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        entry: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "message": message,
            "context": context or {},
            # stalna polja
            "AuditID": self._audit_id,
            "Guid": self._run_guid,
            "JobName": self._job_meta.job_name,
            "JobScheduler": self._job_meta.job_scheduler,
        }
        self._write(entry)

    def info(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        self.log("INFO", message, context)

    def warning(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        self.log("WARNING", message, context)

    def error(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        self.log("ERROR", message, context)

    @abstractmethod
    def _write(self, entry: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def save_to_db(self, entry: Dict[str, Any]) -> None:
        pass


class NullLogger(Logger):
    """Za test/quiet run."""

    def _write(self, entry: Dict[str, Any]) -> None:
        return

    def save_to_db(self, entry: Dict[str, Any]) -> None:
        return
