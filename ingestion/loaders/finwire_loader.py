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

DQ: No silent failures. Every fixed-width field (incl. PTS) is cast through
_safe_cast -- failed casts yield None in the target column plus a structured
error dict. Per-line errors are packed via _pack_dq_errors into _dq_errors
(NULL if clean), written per record type (CMP/SEC/FIN each get their own
_dq_errors on their own row). Unknown RecType is still a hard raise --
structural drift, not a value problem.
"""

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..common import _safe_cast, _pack_dq_errors, compute_row_hash, parse_yyyymmdd, parse_decimal, StreamingCsvWriter
from ..snowflake_client import copy_into

PTS_WIDTH = 15
RECTYPE_WIDTH = 3

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
    ("shout", 13, int),
    ("firsttradedate", 8, parse_yyyymmdd),
    ("firsttradeexchg", 8, parse_yyyymmdd),
    ("dividend", 12, parse_decimal),
]

FIN_FIELDS = [
    ("year", 4, int),
    ("quarter", 1, int),
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
    ("shout", 13, int),
    ("dilutedshout", 13, int),
]


def _split_fixed(line: str, field_specs: List[Tuple[str, int, Any]]) -> Tuple[Dict[str, Any], List[Optional[dict]], str]:
    """
    Slices a fixed-width line, safe-casting every field.
    Returns (values_dict, errors_list, remainder_str).
    """
    offset = PTS_WIDTH + RECTYPE_WIDTH
    values = {}
    errors = []

    for name, width, caster in field_specs:
        raw = line[offset : offset + width].strip()
        offset += width

        value, error = _safe_cast(raw, caster, name)
        values[name] = value
        if error:
            errors.append(error)

    remainder = line[offset:].strip()
    return values, errors, remainder


def _resolve_co_name_or_cik(remainder: str) -> Tuple[Optional[str], Optional[str]]:
    if remainder.isdigit():
        return None, remainder
    return remainder, None


def load_finwire_source(conn: Any, filepath: Path, batch_id: int, tmp_dir: Path) -> int:
    source_file = filepath.name
    loaded_at = datetime.now(timezone.utc)

    cmp_path = tmp_dir / f"finwire_cmp_{filepath.name}_b{batch_id}.csv"
    sec_path = tmp_dir / f"finwire_sec_{filepath.name}_b{batch_id}.csv"
    fin_path = tmp_dir / f"finwire_fin_{filepath.name}_b{batch_id}.csv"

    with StreamingCsvWriter(cmp_path) as cmp_w, \
         StreamingCsvWriter(sec_path) as sec_w, \
         StreamingCsvWriter(fin_path) as fin_w:

        with open(filepath, "r", encoding="utf-8") as f:
            for line_num, raw_line in enumerate(f, start=1):
                line = raw_line.rstrip("\n").rstrip("\r")
                if not line.strip():
                    continue

                pts_raw = line[:PTS_WIDTH].strip()
                rectype = line[PTS_WIDTH : PTS_WIDTH + RECTYPE_WIDTH].strip()
                pts, pts_error = _safe_cast(pts_raw, lambda v: datetime.strptime(v, "%Y%m%d-%H%M%S"), "pts")

                if rectype == "CMP":
                    values, errors, _ = _split_fixed(line, CMP_FIELDS)
                    if pts_error:
                        errors.append(pts_error)
                    row_hash = compute_row_hash(list(values.values()))
                    dq_errors = _pack_dq_errors(errors)
                    cmp_w.write(
                        [pts] + list(values.values()) + [batch_id, source_file, loaded_at, row_hash, dq_errors]
                    )

                elif rectype == "SEC":
                    values, errors, remainder = _split_fixed(line, SEC_FIELDS)
                    if pts_error:
                        errors.append(pts_error)
                    co_name, co_cik = _resolve_co_name_or_cik(remainder)
                    row_hash = compute_row_hash(list(values.values()) + [remainder])
                    dq_errors = _pack_dq_errors(errors)
                    sec_w.write(
                        [pts] + list(values.values()) + [co_name, co_cik, batch_id, source_file, loaded_at, row_hash, dq_errors]
                    )

                elif rectype == "FIN":
                    values, errors, remainder = _split_fixed(line, FIN_FIELDS)
                    if pts_error:
                        errors.append(pts_error)
                    co_name, co_cik = _resolve_co_name_or_cik(remainder)
                    row_hash = compute_row_hash(list(values.values()) + [remainder])
                    dq_errors = _pack_dq_errors(errors)
                    fin_w.write(
                        [pts] + list(values.values()) + [co_name, co_cik, batch_id, source_file, loaded_at, row_hash, dq_errors]
                    )

                else:
                    raise ValueError(f"{filepath.name} line {line_num}: unknown RecType '{rectype}'")

    total_loaded = 0

    if cmp_w.count:
        cols = ["pts"] + [f[0] for f in CMP_FIELDS] + ["_batch_id", "_source_file", "_loaded_at", "_row_hash", "_dq_errors"]
        total_loaded += copy_into(conn, "bronze_finwire_cmp", cols, cmp_path)

    if sec_w.count:
        cols = (["pts"] + [f[0] for f in SEC_FIELDS] + ["coname", "cocik"]
                + ["_batch_id", "_source_file", "_loaded_at", "_row_hash", "_dq_errors"])
        total_loaded += copy_into(conn, "bronze_finwire_sec", cols, sec_path)

    if fin_w.count:
        cols = (["pts"] + [f[0] for f in FIN_FIELDS] + ["coname", "cocik"]
                + ["_batch_id", "_source_file", "_loaded_at", "_row_hash", "_dq_errors"])
        total_loaded += copy_into(conn, "bronze_finwire_fin", cols, fin_path)

    return total_loaded