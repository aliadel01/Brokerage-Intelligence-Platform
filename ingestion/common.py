"""
Shared helpers used across all bronze loaders: type casters, the row-hash
function, and the CSV normalization writer used to stage rows for
COPY INTO (Snowflake loads via stage + COPY INTO, not
row-by-row INSERT).

Function Summary:
- _safe_cast: Returns a tuple of (casted_value, error_info). If casting is successful, error_info is None. If casting fails, casted_value is None and error_info contains details about the failure.
- _pack_dq_errors: Aggregate a row's per-column cast errors into a single JSON string for _dq_errors, or None if clean.
- compute_row_hash: Generate a deterministic 64-bit integer hash from a list of business column values. 
- format_csv_value: Render a Python value into the exact string form the Snowflake file format expects.
- StreamingCsvWriter: Chunked staging CSV writer for loaders that need to fan a single streamed input out to several target tables at once.
- write_staging_csv: Writes rows to a CSV file in fixed-size chunks, bounding peak memory usage.
"""
import csv
import json
import hashlib
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Iterable, List, Optional, Sequence, Tuple

def get_logger(batch_id: int, log_file: str | None = None) -> logging.LoggerAdapter:
    logger = logging.getLogger("bronze_ingestion")
    logger.setLevel(logging.INFO)
 
    if not logger.handlers:  # avoid duplicate handlers on repeated calls
        fmt = logging.Formatter(
            "%(asctime)s [batch %(batch_id)s] %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
 
        console = logging.StreamHandler()
        console.setFormatter(fmt)
        logger.addHandler(console)
 
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(fmt)
            logger.addHandler(file_handler)
 
    # inject batch_id into every record without repeating it at each call site
    return logging.LoggerAdapter(logger, {"batch_id": batch_id})


def compute_row_hash(values: Sequence[Any]) -> int:
    """
    Generate a deterministic 64-bit integer hash from a list of business column values.
    """
    # Defensive Concatenation: Normalize NULLs to empty strings before joining to prevent type mismatches.
    joined = "|".join("" if v is None else str(v) for v in values).encode("utf-8")
    
    # Hash Selection Strategy: BLAKE2b with digest_size=8 generates a native 64-bit hash directly.
    # This avoids the CPU overhead of computing a full 256-bit SHA-256 digest only to truncate it,
    # optimizing execution speed across millions of ingested rows.
    digest = hashlib.blake2b(joined, digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=False)


# Standard Caster Lambdas: Centralized parsing routines ensuring uniform string-to-type conversion rules across all loaders.
parse_date = lambda v: datetime.strptime(v, "%Y-%m-%d").date()
parse_datetime = lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
parse_bool = lambda v: v in ("1", "true", "True", "Y", "y")
parse_decimal = lambda v: Decimal(v).normalize()

# Edge Case Handling: TPC-DI and legacy batch files often represent empty or default dates as '00000000'.
parse_yyyymmdd = lambda v: None if set(v) == {"0"} else datetime.strptime(v, "%Y%m%d").date()


def format_csv_value(value: Any) -> str:
    """
    Render a Python value into the exact string form expected by Snowflake's COPY INTO file format.
    """
    # NULL Alignment: Return empty string to match Snowflake DDL parameters (NULL_IF=('') & EMPTY_FIELD_AS_NULL=TRUE).
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    
    # Precision Truncation: Slices datetime microseconds from 6 digits to 3 digits (.fff)
    # to enforce millisecond precision compatibility with Snowflake TIMESTAMP_NTZ columns.
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


# Memory Boundary Optimization:
# Streaming multi-gigabyte files row-by-row creates Python I/O bottlenecks, while loading 
# an entire file into memory causes OOM (Out Of Memory) errors. 
# Buffering 5,000 rows balances memory consumption (~few hundred KB) with high disk write throughput.
DEFAULT_CHUNK_SIZE = 5000


class StreamingCsvWriter:
    """
    Chunked staging CSV writer for loaders that fan out a single streamed input 
    (e.g., interleaved FINWIRE flat files or XML streams) to multiple target tables simultaneously.
    """

    def __init__(self, path, chunk_size: int = DEFAULT_CHUNK_SIZE):
        self.path = path
        self.count = 0
        self.chunk_size = chunk_size
        self._buffer: List[List[str]] = []
        # Explicit File Protocols: Always use newline="" for Python csv module to prevent double carriage-returns on Windows/Linux environments.
        self._f = open(path, "w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._f, delimiter=",", quoting=csv.QUOTE_MINIMAL)

    def write(self, row: Sequence[Any]) -> None:
        """Buffers formatted rows and triggers a disk flush when chunk size is reached."""
        self.count += 1
        self._buffer.append([format_csv_value(v) for v in row])
        if len(self._buffer) >= self.chunk_size:
            self._flush()

    def _flush(self) -> None:
        """Batch-writes accumulated rows using writerows() for reduced I/O calls, then clears memory."""
        if self._buffer:
            self._writer.writerows(self._buffer)
            self._buffer.clear()

    def close(self) -> None:
        """Ensures remaining buffered rows are flushed to disk before closing the file handle."""
        self._flush()
        self._f.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        """Guarantees resource cleanup even if exceptions are raised during execution."""
        self.close()
        return False


def write_staging_csv(path: str, rows: Iterable[Sequence[Any]],
                      chunk_size: int = DEFAULT_CHUNK_SIZE) -> int:
    """
    Writes sequential row iterables to a CSV staging file using memory-bounded chunking.
    """
    count = 0
    chunk: List[List[str]] = []
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            chunk.append([format_csv_value(v) for v in row])
            count += 1
            if len(chunk) >= chunk_size:
                writer.writerows(chunk)
                chunk.clear()
        
        # Flush Remainder: Writes any residual rows that didn't fill the final chunk.
        if chunk:
            writer.writerows(chunk)

    return count


def _safe_cast(raw: Any, caster: Callable[[str], Any], col_name: str = "") -> tuple[Optional[Any], Optional[dict]]:
    
    """
    Safely cast a raw value to a target type, returning None for empty or invalid values.
    Returns a tuple of (casted_value, error_info). If casting is successful, error_info is None. If casting fails, casted_value is None and error_info contains details about the failure.
    """
    
    if raw is None:
        return None, None

    raw_str = str(raw).strip()
    if raw_str == "":
        return None, None

    try:
        return caster(raw_str), None
    except Exception as e:
        error_info = {
            "column": col_name,
            "raw_value": raw_str,
            "error_type": type(e).__name__,
            "error_msg": str(e),
        }
        return None, error_info

def _pack_dq_errors(errors: List[Optional[dict]]) -> Optional[str]:
    """Aggregate a row's per-column cast errors into a single JSON string for _dq_errors, or None if clean."""
    clean = [e for e in errors if e]
    return json.dumps(clean) if clean else None