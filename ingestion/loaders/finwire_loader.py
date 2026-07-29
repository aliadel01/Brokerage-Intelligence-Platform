"""
Loader for FINWIRE: A fixed-width, no-delimiter quarterly flat file.

FINWIRE files interleave three distinct financial record types within a single
stream, identified by a 3-character `RecType` field positioned after a
15-character PTS (Posting Timestamp) prefix.

Record Types:
- CMP (Company): Metadata about companies (CIK, Industry, Address, CEO, etc.).
- SEC (Security): Financial security details (Symbol, IssueType, Shares, etc.).
- FIN (Financial): Quarterly financial numbers (Revenue, EPS, Assets, etc.).

Pipeline Workflow:
1. Stream file line-by-line to extract fixed-width fields based on predefined schemas.
2. Resolve variable-length trailing fields (`CoNameOrCIK`) for SEC and FIN records.
3. Compute a deterministic 64-bit row hash for data deduplication & lineage.
4. Stage partitioned records into type-specific CSV files.
5. Execute Snowflake bulk loading (`COPY INTO`) for high-throughput ingestion into Bronze layer.
"""

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..common import _safe_cast, compute_row_hash, parse_yyyymmdd, parse_decimal, StreamingCsvWriter
from ..snowflake_client import copy_into

# Fixed-width offset constants (Characters)
PTS_WIDTH = 15
RECTYPE_WIDTH = 3

# Field definitions: (column_name, width_in_chars, caster_function) — names match bronze DDL
CMP_FIELDS = [
    ("companyname", 60, str),
    ("cik", 10, str),
    ("status", 4, str),
    ("industryid", 2, str),
    ("sprating", 4, str),
    ("foundingdate", 8, parse_yyyymmdd),
    ("addrline1", 80, str),
    ("addrline2", 80, str),
    ("postalcode", 12, str),
    ("city", 25, str),
    ("stateprovince", 20, str),
    ("country", 24, str),
    ("ceoname", 46, str),
    ("description", 150, str),
]

SEC_FIELDS = [
    ("symbol", 15, str),
    ("issuetype", 6, str),
    ("status", 4, str),
    ("name", 70, str),
    ("exid", 6, str),
    ("shout", 13, lambda v: _safe_cast(v, int)),
    ("firsttradedate", 8, parse_yyyymmdd),
    ("firsttradeexchg", 8, parse_yyyymmdd),
    ("dividend", 12, parse_decimal),
]

FIN_FIELDS = [
    ("year", 4, lambda v: _safe_cast(v, int)),
    ("quarter", 1, lambda v: _safe_cast(v, int)),
    ("qtrstartdate", 8, parse_yyyymmdd),
    ("postingdate", 8, parse_yyyymmdd),
    ("revenue", 17, parse_decimal),
    ("earnings", 17, parse_decimal),
    ("eps", 12, parse_decimal),
    ("dilutedeps", 12, parse_decimal),
    ("margin", 12, parse_decimal),
    ("inventory", 17, parse_decimal),
    ("assets", 17, parse_decimal),
    ("liabilities", 17, parse_decimal),
    ("shout", 13, lambda v: _safe_cast(v, int)),
    ("dilutedshout", 13, lambda v: _safe_cast(v, int)),
]


def _split_fixed(line: str, field_specs: List[Tuple[str, int, Any]]) -> Tuple[Dict[str, Any], str]:
    """
    Slices a fixed-width line according to the provided field specification schemas.

    Args:
        line: The raw string line read from the FINWIRE file.
        field_specs: List of tuples specifying (field_name, field_width, type_caster).

    Returns:
        Tuple containing:
        - Dict[str, Any]: Extracted field names mapped to their safely cast values.
        - str: Unparsed remainder of the line (used for variable trailing fields).
    """
    # Offset starts right after the fixed header: PTS (15 chars) + RecType (3 chars)
    offset = PTS_WIDTH + RECTYPE_WIDTH
    values = {}
    
    for name, width, caster in field_specs:
        # Fixed-Width Slicing: Extract exact character slice and strip space padding
        raw = line[offset : offset + width].strip()
        offset += width
        
        # Centralized Casting Pattern: Convert valid raw strings or assign None
        values[name] = _safe_cast(raw, caster) if raw else None
        
    # Capture unparsed remainder for downstream variable length handling (e.g., CoNameOrCIK)
    remainder = line[offset:].strip()
    return values, remainder


def _resolve_co_name_or_cik(remainder: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parses the variable trailing field `CoNameOrCIK` in SEC and FIN records.

    The specification dictates that if the remainder string contains purely numeric 
    digits, it represents the CIK (Central Index Key). Otherwise, it represents the 
    Company Name (CoName).

    Args:
        remainder: Trailing unparsed string section of the line.

    Returns:
        Tuple containing (CoName, CoCIK), where one element is populated and 
        the other is None.
    """
    if remainder.isdigit():
        return None, remainder
    return remainder, None


def load_finwire_source(conn: Any, filepath: Path, batch_id: int, tmp_dir: Path) -> int:
    """
    Main loader process for FINWIRE files. 
    
    Reads a single interleaved FINWIRE file, parses lines into three distinct 
    record datasets (CMP, SEC, FIN), stages them as normalized CSVs, and bulk-loads 
    them into Snowflake Bronze tables.

    Args:
        conn: Active Snowflake database connection/cursor.
        filepath: Path object pointing to the raw FINWIRE source file.
        batch_id: Unique pipeline run/execution identifier.
        tmp_dir: Directory path for generating temporary staging CSV files.

    Returns:
        int: Total number of rows successfully ingested across all 3 tables.
    """
    source_file = filepath.name
    loaded_at = datetime.now(timezone.utc)

    cmp_path = tmp_dir / f"finwire_cmp_{filepath.name}_b{batch_id}.csv"
    sec_path = tmp_dir / f"finwire_sec_{filepath.name}_b{batch_id}.csv"
    fin_path = tmp_dir / f"finwire_fin_{filepath.name}_b{batch_id}.csv"

    # Multi-Writer Stream Design:
    # Open three persistent file handles simultaneously. Interleaved lines are routed
    # directly to their corresponding CSV target, keeping memory overhead minimal O(1)
    # regardless of input file size.
    with StreamingCsvWriter(cmp_path) as cmp_w, \
         StreamingCsvWriter(sec_path) as sec_w, \
         StreamingCsvWriter(fin_path) as fin_w:

        with open(filepath, "r", encoding="utf-8") as f:
            for line_num, raw_line in enumerate(f, start=1):
                line = raw_line.rstrip("\n").rstrip("\r")
                if not line.strip():
                    continue

                # Header Extraction: PTS timestamp (15 chars) and RecType discriminator (3 chars)
                pts_raw = line[:PTS_WIDTH].strip()
                rectype = line[PTS_WIDTH : PTS_WIDTH + RECTYPE_WIDTH].strip()
                pts = _safe_cast(pts_raw, lambda v: datetime.strptime(v, "%Y%m%d-%H%M%S"))

                # Dynamic Dispatch Logic
                if rectype == "CMP":
                    values, _ = _split_fixed(line, CMP_FIELDS)
                    row_hash = compute_row_hash(list(values.values()))
                    cmp_w.write(
                        [pts] + list(values.values()) + [batch_id, source_file, loaded_at, row_hash]
                    )

                elif rectype == "SEC":
                    values, remainder = _split_fixed(line, SEC_FIELDS)
                    co_name, co_cik = _resolve_co_name_or_cik(remainder)
                    row_hash = compute_row_hash(list(values.values()) + [remainder])
                    sec_w.write(
                        [pts] + list(values.values()) + [co_name, co_cik, batch_id, source_file, loaded_at, row_hash]
                    )

                elif rectype == "FIN":
                    values, remainder = _split_fixed(line, FIN_FIELDS)
                    co_name, co_cik = _resolve_co_name_or_cik(remainder)
                    row_hash = compute_row_hash(list(values.values()) + [remainder])
                    fin_w.write(
                        [pts] + list(values.values()) + [co_name, co_cik, batch_id, source_file, loaded_at, row_hash]
                    )

                else:
                    raise ValueError(f"{filepath.name} line {line_num}: unknown RecType '{rectype}'")

    total_loaded = 0

    # Bulk Ingestion Phase: Execute COPY INTO for populated staging target files
    if cmp_w.count:
        cols = ["pts"] + [f[0] for f in CMP_FIELDS] + ["_batch_id", "_source_file", "_loaded_at", "_row_hash"]
        total_loaded += copy_into(conn, "bronze_finwire_cmp", cols, cmp_path)

    if sec_w.count:
        cols = (["pts"] + [f[0] for f in SEC_FIELDS] + ["coname", "cocik"]
                + ["_batch_id", "_source_file", "_loaded_at", "_row_hash"])
        total_loaded += copy_into(conn, "bronze_finwire_sec", cols, sec_path)

    if fin_w.count:
        cols = (["pts"] + [f[0] for f in FIN_FIELDS] + ["coname", "cocik"]
                + ["_batch_id", "_source_file", "_loaded_at", "_row_hash"])
        total_loaded += copy_into(conn, "bronze_finwire_fin", cols, fin_path)

    return total_loaded