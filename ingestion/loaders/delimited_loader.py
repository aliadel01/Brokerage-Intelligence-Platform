"""
Generic loader for all delimited (pipe/comma) sources.

Key design point: sources like Trade, HoldingHistory,
WatchHistory, DailyMarket have fewer columns in the Batch1 historical file
than in Batch2/3 incremental files (no CDC_FLAG/CDC_DSN in Batch1). Rather
than hardcode "batch 1 = no CDC" per source, this loader detects it directly
from the field count of each line:
    field_count == base_column_count       -> no CDC columns present, backfill
    field_count == base_column_count + 2   -> CDC columns present, use them
    anything else                          -> hard error (unexpected schema drift)

DQ: No silent failures. Every business field (and _cdc_dsn) is cast through
_safe_cast, which never raises -- a failed cast yields None for the value
plus a structured error dict {column, raw_value, error_type, error_msg}.
All per-row errors are packed via _pack_dq_errors into a single JSON array
written to _dq_errors (NULL if the row is clean). Bronze still gets the row
either way -- rejection/quarantine is a downstream (silver/DQ layer) decision,
not this loader's. Schema drift (wrong field count) still hard-fails via
raise, since that's a structural file-format problem, not a value problem.

Function Summary:
- load_delimited_source(conn, config, filepath, batch_id, tmp_dir): Reads, validates, transforms delimited files, and bulk-loads them into Snowflake.
- _split_cdc(fields, base_names, cdc_capable, filename, line_num): Extracts CDC metadata (CDC_FLAG, CDC_DSN via _safe_cast) or injects backfill defaults based on line field count.
"""
import csv
from datetime import datetime, timezone
from pathlib import Path

from ..common import compute_row_hash, write_staging_csv, _safe_cast, _pack_dq_errors
from ..snowflake_client import copy_into


def load_delimited_source(conn, config: dict, filepath: Path, batch_id: int, tmp_dir: Path) -> int:
    columns = config["columns"]
    base_names = [c[0] for c in columns]
    casters = [c[1] for c in columns]
    cdc_capable = config["cdc_capable"]
    target_table = config["target_table"]
    delimiter = config["delimiter"]

    out_columns = base_names + ["_batch_id", "_source_file", "_loaded_at", "_row_hash", "_dq_errors"]
    if cdc_capable:
        out_columns = ["_cdc_flag", "_cdc_dsn"] + out_columns

    source_file = filepath.name
    loaded_at = datetime.now(timezone.utc)

    def _iter_rows():
        with open(filepath, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            for line_num, fields in enumerate(reader, start=1):
                if not fields or (len(fields) == 1 and fields[0].strip() == ""):
                    continue

                cdc_flag, cdc_dsn, cdc_error, business_fields = _split_cdc(
                    fields, base_names, cdc_capable, filepath.name, line_num
                )

                if len(business_fields) != len(base_names):
                    raise ValueError(
                        f"{filepath.name} line {line_num}: expected {len(base_names)} "
                        f"business columns, got {len(business_fields)}"
                    )

                # Safe-cast every business field, collecting (value, error) pairs
                casted = [
                    _safe_cast(raw, caster, col_name)
                    for raw, caster, col_name in zip(business_fields, casters, base_names)
                ]
                values = [v for v, _ in casted]
                errors = [e for _, e in casted]
                if cdc_error:
                    errors.append(cdc_error)
                row_hash = compute_row_hash(values)
                dq_errors = _pack_dq_errors(errors)

                row = values + [batch_id, source_file, loaded_at, row_hash, dq_errors]
                if cdc_capable:
                    row = [cdc_flag, cdc_dsn] + row
                yield row

    staging_path = tmp_dir / f"{target_table}_{filepath.stem}_b{batch_id}.csv"
    count = write_staging_csv(staging_path, _iter_rows())
    if count == 0:
        return 0
    return copy_into(conn, target_table, out_columns, staging_path)


def _split_cdc(fields, base_names, cdc_capable, filename, line_num):
    n_base = len(base_names)

    if not cdc_capable:
        return None, None, None, fields

    if len(fields) == n_base + 2:
        cdc_dsn, dsn_error = _safe_cast(fields[1], int, "_cdc_dsn")
        return fields[0], cdc_dsn, dsn_error, fields[2:]

    if len(fields) == n_base:
        return "I", 0, None, fields

    raise ValueError(
        f"{filename} line {line_num}: unexpected column count {len(fields)} "
        f"(expected {n_base} or {n_base + 2} for a CDC-capable source)"
    )