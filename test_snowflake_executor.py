from __future__ import annotations

import uuid

from credentials_store import CredentialsStore
from rest_api_export.logging_base import NullLogger

from snowflake_executor import (
    SnowflakeConfig,
    SnowflakeConnectionFactory,
    SnowflakeScriptExecutor,
)


def main():

    logger = NullLogger()

    # ------------------------------------------
    # 1. UČITAJ CREDENTIALS
    # ------------------------------------------
    store = CredentialsStore.load_from_file("credentials.json", logger)

    sf_cred = store.get("Snowflake")
    sf_cfg = SnowflakeConfig.from_credential(sf_cred)

    factory = SnowflakeConnectionFactory(sf_cfg, logger)
    executor = SnowflakeScriptExecutor(factory, logger)

    # ------------------------------------------
    # 2. PARAMETRI
    # ------------------------------------------
    guid = str(uuid.uuid4())

    params = {
        "last_run_id": guid,
        "source_instance": "INTELEX_EU",
    }

    print("Generated GUID:", guid)

    # ------------------------------------------
    # 3. IZVRŠI FAJL
    # ------------------------------------------
    results = executor.execute_file(
        file_path="config/task_query.sql",
        params=params,
    )

    # ------------------------------------------
    # 4. UZMI DRUGI STATEMENT (SELECT)
    # ------------------------------------------
    if len(results) < 2:
        print("Nema drugog statement-a.")
        return

    select_result = results[1]

    print("\n===== SELECT RESULT =====")

    if select_result.rows:
        print("Columns:", select_result.columns)
        print("Row count:", len(select_result.rows))
        for row in select_result.rows:
            print(row)
    else:
        print("0 redova vraćeno.")


if __name__ == "__main__":
    main()
