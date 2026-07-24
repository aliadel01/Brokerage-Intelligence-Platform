"""
Shared helpers used across all bronze loaders: type casters, the row-hash
function, and the CSV normalization writer used to stage rows for
COPY INTO (Snowflake loads via stage + COPY INTO, not
row-by-row INSERT).

Function Summary:
- _safe_cast: Safely cast a raw value to a target type, returning None for empty or invalid values.
- compute_row_hash: Generate a deterministic 64-bit integer hash from a list of business column values. 
- format_csv_value: Render a Python value into the exact string form the Snowflake file format expects.
- StreamingCsvWriter: Chunked staging CSV writer for loaders that need to fan a single streamed input out to several target tables at once.
- write_staging_csv: Writes rows to a CSV file in fixed-size chunks, bounding peak memory usage.
"""
import csv
import hashlib
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Iterable, List, Optional, Sequence


def compute_row_hash(values: Sequence[Any]) -> int:
    """
    Generate a deterministic 64-bit integer hash from a list of business column values.

    Uses blake2b with an 8-byte digest rather than SHA-256 truncated to 8 bytes:
    this is a dedup/lineage hash, not a security boundary, so there's no reason
    to pay for a cryptographic 256-bit digest and then throw away 24 of the
    32 bytes. blake2b(digest_size=8) is materially faster per call and this
    function runs once per ingested row, across every table.
    """
    joined = "|".join("" if v is None else str(v) for v in values).encode("utf-8")
    digest = hashlib.blake2b(joined, digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=False)


# Standard Caster Lambdas
parse_date = lambda v: datetime.strptime(v, "%Y-%m-%d").date()
parse_datetime = lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
parse_bool = lambda v: v in ("1", "true", "True", "Y", "y")
parse_yyyymmdd = lambda v: None if set(v) == {"0"} else datetime.strptime(v, "%Y%m%d").date()


def format_csv_value(value: Any) -> str:
    """
    Render a Python value into the exact string form the Snowflake file
    format (ff_bronze_csv) expects: ISO dates, ISO-ish timestamps,
    TRUE/FALSE for booleans, empty string for NULL (matches NULL_IF=('')
    and EMPTY_FIELD_AS_NULL=TRUE in the file format DDL).
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


# Rows are buffered and flushed in batches of this size rather than either
# (a) writing the whole file's rows in one shot, or (b) issuing one
# writer.writerow() call per row. (a) means peak memory scales with file
# size — risky for multi-GB fact tables. (b) is memory-safe but pays Python
# function-call/formatting overhead per row with nothing amortized. Chunking
# bounds memory to ~chunk_size rows while batching the writes, which is the
# practical middle ground. 5000 rows/chunk is a reasonable default for
# typical bronze row widths (roughly hundreds of KB per chunk); tune per
# table if a source has unusually wide rows.
DEFAULT_CHUNK_SIZE = 5000


class StreamingCsvWriter:
    """
    Chunked staging CSV writer, for loaders that need to fan a single
    streamed input (e.g. one interleaved FINWIRE file, one XML tree) out to
    several target tables at once. Unlike write_staging_csv, which takes a
    single iterable start-to-finish, this lets several writers stay open
    concurrently so no output table's rows need to be fully buffered while
    the others are still being parsed. Internally it batches rows into
    chunks of `chunk_size` before each write, per DEFAULT_CHUNK_SIZE above.
    
    * This class works as a context manager so it can be used in a `with` block.

    Usage:
        with StreamingCsvWriter(path) as w:
            for row in rows:
                w.write(row)
        w.count  # rows written
    """

    def __init__(self, path, chunk_size: int = DEFAULT_CHUNK_SIZE):
        self.path = path
        self.count = 0
        self.chunk_size = chunk_size
        self._buffer: List[List[str]] = []
        self._f = open(path, "w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._f, delimiter=",", quoting=csv.QUOTE_MINIMAL)

    def write(self, row: Sequence[Any]) -> None:
        self.count += 1
        self._buffer.append([format_csv_value(v) for v in row])
        if len(self._buffer) >= self.chunk_size:
            self._flush()

    def _flush(self) -> None:
        if self._buffer:
            self._writer.writerows(self._buffer)
            self._buffer.clear()

    def close(self) -> None:
        self._flush()
        self._f.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


def write_staging_csv(path: str, rows: Iterable[Sequence[Any]],
                       chunk_size: int = DEFAULT_CHUNK_SIZE) -> int:
    """
    Writes rows to a CSV file in fixed-size chunks: each chunk is formatted
    in memory and flushed with a single writerows() call, then discarded.
    Bounds peak memory to `chunk_size` rows regardless of source file size,
    while batching writes instead of issuing one per row.
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
        if chunk:
            writer.writerows(chunk)

    return count


def _safe_cast(raw: Any, caster: Callable[[str], Any]) -> Optional[Any]:
    """
    Safely casts a raw value using the provided caster function, 
    returning None for empty or invalid values.
    """
    if raw is None:
        return None
    
    raw = str(raw).strip()
    if raw == "":
        return None
        
    try:
        return caster(raw)
    except Exception:
        return None