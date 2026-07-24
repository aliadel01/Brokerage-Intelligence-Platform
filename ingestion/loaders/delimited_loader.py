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

Function Summary:
- load_delimited_source(conn, config, filepath, batch_id, tmp_dir): Reads, validates, transforms delimited files, and bulk-loads them into Snowflake.
- _split_cdc(fields, base_names, cdc_capable, filename, line_num): Extracts CDC metadata (CDC_FLAG, CDC_DSN) or injects backfill defaults based on line field count.
- _safe_cast(raw, caster): Strips whitespace and safely casts a string field using a provided converter, returning None for empty strings.
"""
import csv
from datetime import datetime, timezone
from pathlib import Path

from ..common import compute_row_hash, write_staging_csv, _safe_cast
from ..snowflake_client import copy_into


def load_delimited_source(conn, config: dict, filepath: Path, batch_id: int, tmp_dir: Path) -> int:
    """
    Load a delimited (CSV/PSV) source file into Snowflake.
    """
    columns = config["columns"]
    base_names = [c[0] for c in columns]
    casters = [c[1] for c in columns]
    cdc_capable = config["cdc_capable"]
    target_table = config["target_table"]
    delimiter = config["delimiter"]

    # Schema Projection: Build dynamic destination columns. Append operational metadata fields 
    # to maintain strict auditing and row lineage in Snowflake.
    out_columns = base_names + ["_batch_id", "_source_file", "_loaded_at", "_row_hash"]
    if cdc_capable:
        out_columns = ["_cdc_flag", "_cdc_dsn"] + out_columns

    source_file = filepath.name
    loaded_at = datetime.now(timezone.utc)

    def _iter_rows():
        # Memory-Efficient Generator: Stream-parses records line-by-line rather than 
        # allocating entire multi-gigabyte files into memory, eliminating Out-Of-Memory risks.
        with open(filepath, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            for line_num, fields in enumerate(reader, start=1):
                # Defensive Parsing: Ignore completely empty or whitespace-only lines to prevent downstream errors.
                if not fields or (len(fields) == 1 and fields[0].strip() == ""):
                    continue  # skip blank lines

                # Dynamic CDC Resolution: Handles historical vs incremental schema variations on a per-line basis.
                cdc_flag, cdc_dsn, business_fields = _split_cdc(
                    fields, base_names, cdc_capable, filepath.name, line_num
                )

                # Schema Validation: Validate business column counts upfront before processing types.
                if len(business_fields) != len(base_names):
                    raise ValueError(
                        f"{filepath.name} line {line_num}: expected {len(base_names)} "
                        f"business columns, got {len(business_fields)}"
                    )

                # Data Normalization & Hash Calculation:
                # 1. Cast raw string fields safely using type-specific converters.
                # 2. Compute BLAKE2b hash across original raw business fields to capture state for future CDC comparisons.
                values = [_safe_cast(raw, caster) for raw, caster in zip(business_fields, casters)]
                row_hash = compute_row_hash(business_fields)

                row = values + [batch_id, source_file, loaded_at, row_hash]
                if cdc_capable:
                    row = [cdc_flag, cdc_dsn] + row
                yield row

    staging_path = tmp_dir / f"{target_table}_{filepath.stem}_b{batch_id}.csv"
    
    # Execution Guard: Write staging file in chunks and trigger bulk load only if valid records exist.
    count = write_staging_csv(staging_path, _iter_rows())
    if count == 0:
        return 0
    return copy_into(conn, target_table, out_columns, staging_path)


def _split_cdc(fields, base_names, cdc_capable, filename, line_num):
    """
    Inspect a line's field count and extract or default its CDC attributes.
    """
    n_base = len(base_names)

    # Static Sources: Pass raw fields straight through if the source definition isn't CDC-tracked.
    if not cdc_capable:
        return None, None, fields

    # Incremental Batch Pattern: CDC columns (`_cdc_flag`, `_cdc_dsn`) are prepended at indices 0 and 1.
    if len(fields) == n_base + 2:
        return fields[0], int(fields[1]), fields[2:]

    # Historical Backfill Pattern (Batch 1):
    # Files in Batch 1 lack CDC headers. Automatically backfill default values 
    # ('I' = Insert, 0 = DSN baseline) to align schemas without breaking execution.
    if len(fields) == n_base:
        return "I", 0, fields

    # Schema Drift Guard: Throw an explicit exception if the line length doesn't match expected historical or incremental specs.
    raise ValueError(
        f"{filename} line {line_num}: unexpected column count {len(fields)} "
        f"(expected {n_base} or {n_base + 2} for a CDC-capable source)"
    )