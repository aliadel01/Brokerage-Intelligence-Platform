"""
Generic loader for all delimited (pipe/comma) sources.

Key design point (see ADR-001): sources like Trade, HoldingHistory,
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

    Operational Steps:
    1. Extract table schema, data casters, delimiter, and CDC capability flags from the configuration dictionary.
    2. Build target output column definitions including system metadata fields (_batch_id, _source_file, _loaded_at, _row_hash).
    3. Stream lines from the input file, ignoring empty/blank lines.
    4. Resolve CDC flags/DSN values per line via _split_cdc() and enforce schema column counts.
    5. Cast raw field values to target data types and generate a deterministic row hash for QA.
    6. Stage normalized rows into a local CSV file in tmp_dir.
    7. Execute PUT + COPY INTO to bulk load staged rows into the target Snowflake table.

    Returns:
        int: Total number of rows successfully loaded into Snowflake.
    """
    columns = config["columns"]
    base_names = [c[0] for c in columns]
    casters = [c[1] for c in columns]
    cdc_capable = config["cdc_capable"]
    target_table = config["target_table"]
    delimiter = config["delimiter"]

    out_columns = base_names + ["_batch_id", "_source_file", "_loaded_at", "_row_hash"]
    if cdc_capable:
        out_columns = ["_cdc_flag", "_cdc_dsn"] + out_columns

    source_file = filepath.name
    loaded_at = datetime.now(timezone.utc)

    def _iter_rows():
        # Generator, not a list: write_staging_csv streams rows straight
        # through to disk one at a time, so the whole source file never
        # has to sit fully parsed in memory at once (important for the
        # multi-GB fact tables like Trade/CashTransaction/DailyMarket).
        with open(filepath, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            for line_num, fields in enumerate(reader, start=1):
                if not fields or (len(fields) == 1 and fields[0].strip() == ""):
                    continue  # skip blank lines

                cdc_flag, cdc_dsn, business_fields = _split_cdc(
                    fields, base_names, cdc_capable, filepath.name, line_num
                )

                if len(business_fields) != len(base_names):
                    raise ValueError(
                        f"{filepath.name} line {line_num}: expected {len(base_names)} "
                        f"business columns, got {len(business_fields)}"
                    )

                values = [_safe_cast(raw, caster) for raw, caster in zip(business_fields, casters)]
                row_hash = compute_row_hash(business_fields)

                row = values + [batch_id, source_file, loaded_at, row_hash]
                if cdc_capable:
                    row = [cdc_flag, cdc_dsn] + row
                yield row

    staging_path = tmp_dir / f"{target_table}_{filepath.stem}_b{batch_id}.csv"
    count = write_staging_csv(staging_path, _iter_rows())
    if count == 0:
        return 0
    return copy_into(conn, target_table, out_columns, staging_path)


def _split_cdc(fields, base_names, cdc_capable, filename, line_num):
    """
    Inspect a line's field count and extract or default its CDC attributes.

    Operational Steps:
    1. If source is not CDC capable, return None for CDC attributes and pass back raw fields.
    2. If field count equals base columns + 2, extract CDC_FLAG and CDC_DSN directly from the first two positions.
    3. If field count equals base columns only (Batch 1 historical pattern), inject backfill defaults ('I' for insert, 0 for DSN).
    4. Raise ValueError if field count matches neither pattern (detects schema drift).

    Returns:
        tuple: (cdc_flag, cdc_dsn, business_fields)
    """
    n_base = len(base_names)

    if not cdc_capable:
        return None, None, fields

    if len(fields) == n_base + 2:
        return fields[0], int(fields[1]), fields[2:]

    if len(fields) == n_base:
        # Batch1-style row with no CDC columns -> backfill per ADR-001.
        return "I", 0, fields

    raise ValueError(
        f"{filename} line {line_num}: unexpected column count {len(fields)} "
        f"(expected {n_base} or {n_base + 2} for a CDC-capable source)"
    )