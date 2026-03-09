from __future__ import annotations

import time
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional

from rest_api_export.logging_base import Logger


@dataclass(frozen=True)
class JobStartResult:
    audit_id: int


class JobMonitor(ABC):
    def __init__(self, logger: Logger):
        self._log = logger

    @abstractmethod
    def start_job(self, context: Optional[Dict[str, Any]] = None) -> JobStartResult:
        raise NotImplementedError

    @abstractmethod
    def finish_job(self, audit_id: int, success: bool, context: Optional[Dict[str, Any]] = None) -> None:
        raise NotImplementedError


class NullJobMonitor(JobMonitor):
    """
    Školjka: generiše int AuditID dok ne ubacimo pravu bazu/skripte.
    """

    def start_job(self, context: Optional[Dict[str, Any]] = None) -> JobStartResult:
        # call Snowflake script to get AuditID and insert row in snowflake monitorng database
        audit_id = 1

        self._log.info("JobMonitor.start_job (local)", {"audit_id": audit_id, "context": context or {}})
        return JobStartResult(audit_id=audit_id)

    def finish_job(self, audit_id: int, success: bool, context: Optional[Dict[str, Any]] = None) -> None:
        self._log.info("JobMonitor.finish_job (local)", {"audit_id": audit_id, "success": success, "context": context or {}})
        # call Snowflake script to update status use context: Optional[Dict[str, Any]] to provde status, message etc.
