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

from ..common import _safe_cast, compute_row_hash, parse_yyyymmdd, StreamingCsvWriter
from ..snowflake_client import copy_into

# Fixed-width offset constants (Characters)
PTS_WIDTH = 15
RECTYPE_WIDTH = 3

# Field definitions: (Field_Name, Width_In_Chars, Caster_Function)
CMP_FIELDS = [
    ("CompanyName", 60, str),
    ("CIK", 10, str),
    ("Status", 4, str),
    ("IndustryID", 2, str),
    ("SPrating", 4, str),
    ("FoundingDate", 8, parse_yyyymmdd),
    ("AddrLine1", 80, str),
    ("AddrLine2", 80, str),
    ("PostalCode", 12, str),
    ("City", 25, str),
    ("StateProvince", 20, str),
    ("Country", 24, str),
    ("CEOname", 46, str),
    ("Description", 150, str),
]

SEC_FIELDS = [
    ("Symbol", 15, str),
    ("IssueType", 6, str),
    ("Status", 4, str),
    ("Name", 70, str),
    ("ExID", 6, str),
    ("ShOut", 13, lambda v: _safe_cast(v, int)),
    ("FirstTradeDate", 8, parse_yyyymmdd),
    ("FirstTradeExchg", 8, parse_yyyymmdd),
    ("Dividend", 12, Decimal),
]

FIN_FIELDS = [
    ("Year", 4, lambda v: _safe_cast(v, int)),
    ("Quarter", 1, lambda v: _safe_cast(v, int)),
    ("QtrStartDate", 8, parse_yyyymmdd),
    ("PostingDate", 8, parse_yyyymmdd),
    ("Revenue", 17, Decimal),
    ("Earnings", 17, Decimal),
    ("EPS", 12, Decimal),
    ("DilutedEPS", 12, Decimal),
    ("Margin", 12, Decimal),
    ("Inventory", 17, Decimal),
    ("Assets", 17, Decimal),
    ("Liabilities", 17, Decimal),
    ("ShOut", 13, lambda v: _safe_cast(v, int)),
    ("DilutedShOut", 13, lambda v: _safe_cast(v, int)),
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
    offset = PTS_WIDTH + RECTYPE_WIDTH
    values = {}
    
    for name, width, caster in field_specs:
        # Extract slice and trim padding spaces
        raw = line[offset : offset + width].strip()
        offset += width
        
        # Apply type casting via centralized safe casting pattern
        values[name] = _safe_cast(raw, caster) if raw else None
        
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

    # Three writers stay open across the single pass over the file so each
    # record type is flushed to its own staging CSV as soon as it's parsed,
    # rather than accumulating cmp_rows/sec_rows/fin_rows in memory for the
    # whole file (FINWIRE quarterly files interleave all three types, so the
    # old approach held the full parsed file 3x over in RAM before writing
    # anything).
    with StreamingCsvWriter(cmp_path) as cmp_w, \
         StreamingCsvWriter(sec_path) as sec_w, \
         StreamingCsvWriter(fin_path) as fin_w:

        with open(filepath, "r", encoding="utf-8") as f:
            for line_num, raw_line in enumerate(f, start=1):
                line = raw_line.rstrip("\n").rstrip("\r")
                if not line.strip():
                    continue

                # Extract fixed header offsets (PTS + RecType)
                pts_raw = line[:PTS_WIDTH].strip()
                rectype = line[PTS_WIDTH : PTS_WIDTH + RECTYPE_WIDTH].strip()
                pts = _safe_cast(pts_raw, lambda v: datetime.strptime(v, "%Y%m%d-%H%M%S"))

                # Dispatch line parsing based on Record Type
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

    if cmp_w.count:
        cols = ["PTS"] + [f[0] for f in CMP_FIELDS] + ["_batch_id", "_source_file", "_loaded_at", "_row_hash"]
        total_loaded += copy_into(conn, "bronze_finwire_cmp", cols, cmp_path)

    if sec_w.count:
        cols = (["PTS"] + [f[0] for f in SEC_FIELDS] + ["CoName", "CoCIK"]
                + ["_batch_id", "_source_file", "_loaded_at", "_row_hash"])
        total_loaded += copy_into(conn, "bronze_finwire_sec", cols, sec_path)

    if fin_w.count:
        cols = (["PTS"] + [f[0] for f in FIN_FIELDS] + ["CoName", "CoCIK"]
                + ["_batch_id", "_source_file", "_loaded_at", "_row_hash"])
        total_loaded += copy_into(conn, "bronze_finwire_fin", cols, fin_path)

    return total_loaded