from __future__ import annotations

from pathlib import Path
from rest_api_export.json_file_logger import JsonFileLogger
from parser_factory import ParserFactory
from reader_factory import ReaderFactory
from parquet_exporter import ParquetExporter, ParquetConfig
from task_executor import TaskExecutor
from credentials_store import CredentialsStore
from azure_blob_uploader import AzureBlobUploader
from azure_blob_archiver import AzureBlobArchiver, AzureBlobArchiveConfig
from snowflake_code_generator import SnowflakeCodeGenerator


def main() -> None:
    # -------------------------------------------------
    # Job meta (za sada hardcode; kasnije CLI/env)
    # -------------------------------------------------
    job_name = "INTELEX_EU"
    job_scheduler = "Manual"
    code_generator = False
    delete_before_upload=True

    # 1) Logger
    logger = JsonFileLogger("logs/run_{audit_id}.jsonl")
    logger.set_job_meta(job_name=job_name, job_scheduler=job_scheduler)

    audit_id = logger.start_job(query_file_path="config/start_job.sql",
        credentials_path="credentials.json",
        credential_name="Snowflake", context=None)
    logger.set_audit_id(str(audit_id))

    logger.info("Main started", {"job_name": job_name, "job_scheduler": job_scheduler})

    # 2) Credentials store
    creds = CredentialsStore.load_from_file("credentials.json", logger=logger)

    # 3) Factories
    parser_factory = ParserFactory(logger)
    reader_factory = ReaderFactory(creds, parser_factory, logger)

    parquet = ParquetExporter(
        ParquetConfig(compression="snappy"),
        logger=logger,
    )

    # -------------------------------------------------
    # 4) Azure uploader
    # -------------------------------------------------
    ab_data_cred = creds.get("AB_Data")

    if delete_before_upload:

        ab_archive_cred = creds.get("AB_Archive")


        archiver = AzureBlobArchiver(logger,ab_data_cred, 'raw', ab_archive_cred,'archive')
        archiver.delete_directory(
            container_type="source",
            directory="INTELEX_EU/"
        )

    data_uploader = AzureBlobUploader(ab_data_cred, logger)

    # 5) Executor
    executor = TaskExecutor(
        reader_factory=reader_factory,
        parquet=parquet,
        logger=logger,
        azure_uploader=data_uploader,
        is_load_from_config=False,
    )

    # 6) Load + run
    #tasks = executor.load_tasks("tasks.json")

    tasks = executor.load_tasks_from_snowflake(
        query_file_path="config/task_query.sql",
        credentials_path="credentials.json",
        credential_name="Snowflake",
        params={"audit_id": logger.get_audit_id(), "source_instance": "INTELEX_EU", "last_run_id": logger.get_run_guid()},
    )

    try:
        executor.run_tasks(tasks)

        if code_generator:
            base_path = Path(__file__).parent / ""

            tasks_path = base_path / executor.get_materialized_task_file()
            credentials_path = base_path / "credentials.json"

            generator = SnowflakeCodeGenerator(
                audit_id=int(logger.get_audit_id()),
                tasks_path=tasks_path,
                credentials_path=credentials_path
            )

            generator.run()
    finally:
        try:
            synced = executor.sync_tasks_to_snowflake(
                credentials_path="credentials.json",
                credential_name="Snowflake",
                # target_table="MONITORING.CONTROLS.REPLICATION_ENTITY_REST_API_V1",
            )
            logger.info("Sync to Snowflake finished", {
                "synced": synced,
                "task_file": executor.get_materialized_task_file(),
            })
        except Exception as e:
            # ne ruši pipeline zbog sync-a
            logger.error("Sync to Snowflake failed", {
                "error": str(e),
                "task_file": executor.get_materialized_task_file(),
            })

    logger.finish_job(success=True, query_file_path="config/finish_job.sql",
        credentials_path="credentials.json",
        credential_name="Snowflake", context=None)


    # -------------------------------------------------
    # 7) Upload logs + cloned task config (iz tasks[].bronze.logs)
    #    i optional local cleanup (log + Execution_Tasks_*.json)
    # -------------------------------------------------

    delete_local_files = any(bool(getattr(t, "delete_local_file", False)) for t in tasks)

    logs_cfg = None
    for t in tasks:
        if getattr(t, "bronze", None) and getattr(t.bronze, "logs", None):
            if str(t.bronze.logs.connection or "").strip():
                logs_cfg = t.bronze.logs
                break

    materialized_task_local_path = executor.get_materialized_task_file()

    if logs_cfg:
        ab_logs_cred = creds.get(str(logs_cfg.connection))
        logs_uploader = AzureBlobUploader(ab_logs_cred, logger)

        logger.upload_and_cleanup_logs(
            uploader=logs_uploader,
            logs_directory=str(logs_cfg.directory or ""),
            execution_log_filename=str(logs_cfg.execution_log_filename or ""),
            materialized_task_local_path=materialized_task_local_path,
            task_config_filename=str(logs_cfg.task_config_filename or ""),
            delete_local_files=delete_local_files,
        )

    print(logger.get_error_message())

if __name__ == "__main__":
    main()
