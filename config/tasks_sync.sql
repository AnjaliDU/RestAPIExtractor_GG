UPDATE MONITORING.CONTROLS.REPLICATION_ENTITY_REST_API_V1
SET
    STATUS_ID = %(status_id)s,
    RUNNING_STATUS = %(running_status)s,
    LAST_RUN_ID = %(last_run_id)s,
    LAST_ERROR_MESSAGE = %(last_error_message)s,

    SOURCE_TYPE = %(source_type)s,
    POOL_ID = %(pool_id)s,
    ORDER_ID = %(order_id)s,
    FORCE_SNOWFLAKE = %(force_snowflake)s,
    DELTA_INDICATOR = %(delta_indicator)s,
    KEY_COLUNS = %(key_coluns)s,

    SOURCE_CONNECTION_DETAILS = %(source_connection_details)s,
    RESPONSE_FORMAT = %(response_format)s,
    SOURCE_METADATA_SERVICE_URL = %(source_metadata_service_url)s,
    SOURCE_URL_INIT = %(source_url_init)s,
    SOURCE_DELTA_URL = %(source_delta_url)s,
    SELECTION_JSON = TRY_PARSE_JSON(%(selection_json)s),

    NEXT_PAGE_ATTRIBUTE = %(next_page_attribute)s,
    NEXT_PAGE_INSTRUCTION = %(next_page_instruction)s,
    DELTA_INCREMENT_ATTRIBUTE = %(delta_increment_attribute)s,
    DELTA_INCREMENT_INSTRUCTION = %(delta_increment_instruction)s,

    STAGE_AZURE_BLOB_CREDENTIAL = %(stage_azure_blob_credential)s,
    STAGE_CONTAINER = %(stage_container)s,
    STAGE_DIRECTORY = %(stage_directory)s,
    PARQUET_FILENAME = %(parquet_filename)s,

    ADD_AUDIT_ID = %(add_audit_id)s,
    ADD_ENTITY_ID = %(add_entity_id)s,
    ADD_EXTRACTED_AT_UTC = %(add_extracted_at_utc)s,

    LOG_CONNECTION = %(log_connection)s,
    LOG_CONTAINER = %(log_container)s,
    LOG_DIRECTORY = %(log_directory)s,
    LOG_TASK_CONFIG_FILENAME = %(log_task_config_filename)s,
    LOG_EXECUTION_FILENAME = %(log_execution_filename)s,

    ARCHIVE_ENABLED = %(archive_enabled)s,
    ARCHIVE_CONNECTION = %(archive_connection)s,
    ARCHIVE_CONTAINER = %(archive_container)s,
    ARCHIVE_OVERWRITE = %(archive_overwrite)s,
    ARCHIVE_DELETE_SOURCE_AFTER = %(archive_delete_source_after)s,

    LOCAL_PARQUET_PATH = %(local_parquet_path)s,
    DELETE_LOCAL_FILE = %(delete_local_file)s,

    DESTINATION_CONNECTION_DETAILS = %(destination_connection_details)s,
    DESTINATION_LANDING_DATABASE = %(destination_landing_database)s,
    DESTINATION_LANDING_SCHEMA = %(destination_landing_schema)s,
    DESTINATION_LANDING_TABLE = %(destination_landing_table)s,
    DESTINATION_DATABASE = %(destination_database)s,
    DESTINATION_SCHEMA = %(destination_schema)s,
    DESTINATION_TABLE = %(destination_table)s,

    DESTINATION_LANDING_CREATE_TABLE_SCRIPT = %(destination_landing_create_table_script)s,
    DESTINATION_CREATE_TABLE_SCRIPT = %(destination_create_table_script)s,
    MERGE_APPLY_INCREMENT_SCRIPT = %(merge_apply_increment_script)s

WHERE ENTITY_ID = %(entity_id)s
  AND SOURCE_INSTANCE = %(source_instance)s
;