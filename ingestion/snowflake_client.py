"""
Snowflake connection handling and the stage-and-load primitive every
loader uses. Snowflake's efficient path for file-based sources is
PUT (local file -> internal stage) + COPY INTO (stage -> table), not
row-by-row INSERT.
"""
from pathlib import Path

import snowflake.connector


def get_connection(args):
    return snowflake.connector.connect(
        account=args.account,
        user=args.user,
        password=args.password,
        role=args.role,
        warehouse=args.warehouse,
        database=args.database,
        schema=args.schema,
    )


def copy_into(conn, table_name: str, columns: list, local_path: Path,
              stage_name: str = "ingest_stage",
              file_format_name: str = "ff_bronze_csv") -> int:
    """
    Stage a local normalized CSV and COPY INTO the target table with an
    explicit column list (never positional-only — keeps loader output and
    table shape from silently drifting apart).
    Returns the number of rows loaded, per Snowflake's COPY INTO result.
    """
    filename = local_path.name
    col_list = ", ".join(columns)

    cur = conn.cursor()
    try:
        clean_path = str(local_path).replace("\\", "/")
        cur.execute(
            f"PUT 'file://{clean_path}' @{stage_name} "
            f"OVERWRITE = TRUE AUTO_COMPRESS = TRUE"
        )
        cur.execute(f"""
            COPY INTO {table_name} ({col_list})
            FROM @{stage_name}
            FILES = ('{filename}.gz')
            FILE_FORMAT = (FORMAT_NAME = '{file_format_name}')
            ON_ERROR = 'ABORT_STATEMENT'
            PURGE = TRUE
        """)
        results = cur.fetchall()
    finally:
        cur.close()

    # COPY INTO result columns include rows_loaded at a fixed position;
    # sum across result rows in case Snowflake split the file internally.
    rows_loaded = sum(r[3] for r in results) if results else 0
    return rows_loaded