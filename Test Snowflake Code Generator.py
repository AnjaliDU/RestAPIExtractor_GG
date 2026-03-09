from pathlib import Path
import json
from typing import Optional
from urllib.parse import urlparse

from rest_api_export.json_file_logger import JsonFileLogger
from helper.snowflake_helper import SnowflakeScriptGenerator
from task_models import ExecutionTask


def render_parquet_path(template_path: Optional[str], audit_id: str) -> Optional[str]:
    if not template_path:
        return None
    return (
        template_path
        .replace("{audit_id}", audit_id)
        .replace("{AuditID}", audit_id)
        .replace("{AUDIT_ID}", audit_id)
    )


def guess_sample_parquet(task_dict: dict, audit_id: str) -> Optional[str]:
    p = task_dict.get("parquet_path")
    if p:
        return render_parquet_path(p, audit_id)

    bronze = task_dict.get("bronze") or {}
    landing = bronze.get("landing") or {}

    filename = landing.get("parquet_filename")
    directory = landing.get("directory") or ""

    if filename:
        dir_part = directory.strip("/")
        combined = f"{dir_part}/{filename}" if dir_part else filename
        return render_parquet_path(combined, audit_id)

    return None


def build_azure_blob_from_sas(sas_uri: str, directory: str) -> dict:
    """
    Iz SAS URI izvlači:
      - account_url
      - container
      - sas_token
    prefix uzima iz bronze.landing.directory
    """
    parsed = urlparse(sas_uri)

    account_url = f"{parsed.scheme}://{parsed.netloc}"

    # path format: /container[/optional_prefix]
    path_parts = [p for p in parsed.path.split("/") if p]
    if not path_parts:
        raise ValueError("Invalid SAS URI - container missing")

    container = path_parts[0]

    sas_token = parsed.query  # bez '?'

    return {
        "account_url": account_url,
        "container": container,
        "prefix": directory.strip("/"),
        "sas_token": sas_token
    }


# ---------------- MAIN ----------------
job_name='Metadata Generator'
job_scheduler='Manual'

logger = JsonFileLogger("logs/scripts_gen.jsonl")
logger.set_job_meta(job_name=job_name, job_scheduler=job_scheduler)

# 2) AuditID (lokalni test)
audit_id = "332"
logger.set_audit_id(audit_id)

gen = SnowflakeScriptGenerator(logger)

exec_path = Path("Execution_Tasks_332.json")
creds_path = Path("credentials.json")

tasks_json = json.loads(exec_path.read_text(encoding="utf-8"))
creds_raw = json.loads(creds_path.read_text(encoding="utf-8"))

credentials_section = creds_raw.get("credentials", {})

for idx, task_dict in enumerate(tasks_json):
    try:
        task_obj = ExecutionTask.from_dict(task_dict)

        # ---- Azure SAS extraction ----
        landing = task_obj.bronze.landing
        cred_key = landing.connection  # npr "AB_Data"

        if cred_key not in credentials_section:
            raise ValueError(f"Credential '{cred_key}' not found in credentials.json")

        cred = credentials_section[cred_key]

        if cred.get("auth_type") != "azure_blob_sas":
            raise ValueError(f"Credential '{cred_key}' is not azure_blob_sas")

        sas_uri = cred.get("sas_uri")
        if not sas_uri:
            raise ValueError(f"sas_uri missing for credential '{cred_key}'")

        azure_blob = build_azure_blob_from_sas(
            sas_uri=sas_uri,
            directory=landing.directory
        )

        # ---- Sample parquet ----
        sample_parq = guess_sample_parquet(task_dict, audit_id)

        # ---- Generiši skripte ----
        scripts = gen.generate_for_execution_task(
            exec_task=task_obj,
            credentials={cred_key: azure_blob},
            sample_parquet_path=sample_parq
        )

        # ---- Upis nazad u JSON ----
        sf = task_dict.setdefault("snowflake", {})
        scr = sf.setdefault("scripts", {})

        scr["landing_view_create"] = scripts.landing_view_create
        scr["replica_table_create"] = scripts.replica_table_create
        scr["merge_apply"] = scripts.merge_apply

        logger.info(
            "Scripts generated",
            {"index": idx, "entity_id": task_obj.entity.entity_id}
        )

    except Exception as e:
        logger.error(
            "Script generation failed",
            {"index": idx, "error": str(e)}
        )

exec_path.write_text(
    json.dumps(tasks_json, indent=2, ensure_ascii=False),
    encoding="utf-8"
)

logger.info("Execution_Tasks_1001.json updated successfully")