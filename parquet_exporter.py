from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Dict, Any

import pyarrow as pa
import pyarrow.parquet as pq

from rest_api_export.logging_base import Logger


@dataclass(frozen=True)
class ParquetConfig:
    compression: Optional[str] = "snappy"
    use_dictionary: bool = True


class ParquetExporter:
    def __init__(self, cfg: ParquetConfig, logger: Logger):
        self._cfg = cfg
        self._log = logger

    def write_batches_old(
        self,
        parquet_path: str,
        batches: Iterable[List[Dict[str, Any]]],
        audit_id: Optional[int] = None,
    ) -> Optional[str]:
        writer: Optional[pq.ParquetWriter] = None
        total = 0

        response_url = None
        try:

            for batch, url in batches:
                response_url=url
                if not batch:
                    continue

                if audit_id is not None:
                    for row in batch:
                        row["AUDIT_ID"] = int(audit_id)

                table = pa.Table.from_pylist(batch)

                if writer is None:
                    writer = pq.ParquetWriter(
                        parquet_path,
                        table.schema,
                        compression=self._cfg.compression,
                        use_dictionary=self._cfg.use_dictionary,
                    )

                writer.write_table(table)
                total += len(batch)
                self._log.info(
                    "Parquet batch written",
                    {"path": parquet_path, "batch_rows": len(batch), "total_rows": total, "audit_id": audit_id},
                )

        finally:
            if writer is not None:
                writer.close()

        self._log.info("Parquet export finished", {"path": parquet_path, "total_rows": total, "audit_id": audit_id})
        return response_url

    from typing import Any, Dict, Iterable, List, Optional, Tuple
    import pyarrow as pa
    import pyarrow.parquet as pq

    def write_batches(
            self,
            parquet_path: str,
            batches: Iterable[Tuple[List[Dict[str, Any]], Optional[str]]],
            audit_id: Optional[int] = None,
    ) -> Optional[str]:
        """
        Upisuje sve batch-eve u Parquet i vraća delta_url (poslednji nenull).
        Stabilizuje schema između batch-eva (sprečava string vs null drift),
        i izbacuje __metadata kolonu.
        """

        writer: Optional[pq.ParquetWriter] = None
        total = 0
        last_delta_url: Optional[str] = None
        fixed_schema: Optional[pa.Schema] = None

        def _drop_metadata(rows: List[Dict[str, Any]]) -> None:
            # inplace: izbaci __metadata iz svakog reda ako postoji
            for r in rows:
                if "__metadata" in r:
                    del r["__metadata"]

        def _fix_null_fields(schema: pa.Schema) -> pa.Schema:
            # Ako je neka kolona inferred kao null (ceo batch None), prebaci je na string
            fields = []
            for f in schema:
                if pa.types.is_null(f.type):
                    fields.append(pa.field(f.name, pa.string(), nullable=True))
                else:
                    fields.append(f)
            return pa.schema(fields)

        try:
            for batch, delta_url in batches:
                if delta_url:
                    last_delta_url = delta_url

                if not batch:
                    continue

                # 1) drop __metadata
                _drop_metadata(batch)

                # 2) AUDIT_ID
                if audit_id is not None:
                    for row in batch:
                        row["AUDIT_ID"] = int(audit_id)

                # 3) Create table with stable schema
                if writer is None:
                    # prvi batch: infer schema
                    table0 = pa.Table.from_pylist(batch)

                    # stabilizuj schema: null -> string
                    fixed_schema = _fix_null_fields(table0.schema)

                    # rebuild table po fixed schema (da se i prvi batch uklopi)
                    table = pa.Table.from_pylist(batch, schema=fixed_schema)

                    writer = pq.ParquetWriter(
                        parquet_path,
                        fixed_schema,
                        compression=self._cfg.compression,
                        use_dictionary=self._cfg.use_dictionary,
                    )
                else:
                    # sledeći batch-evi: uvek koristi fixed schema
                    assert fixed_schema is not None
                    table = pa.Table.from_pylist(batch, schema=fixed_schema)

                writer.write_table(table)

                total += len(batch)
                self._log.info(
                    "Parquet batch written",
                    {"path": parquet_path, "batch_rows": len(batch), "total_rows": total, "audit_id": audit_id},
                )

        finally:
            if writer is not None:
                writer.close()

        self._log.info(
            "Parquet export finished",
            {"path": parquet_path, "total_rows": total, "audit_id": audit_id, "delta_url": last_delta_url},
        )

        return last_delta_url