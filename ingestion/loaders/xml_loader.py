"""
Loader module for ingesting CustomerMgmt.xml into Snowflake Bronze Layer.

Architectural Design Records:
    - Flattening nested XML into relational tables. Nested XML structure 
      (<Action> -> <Customer> -> <Account>) lacks a natural single-table tabular 
      representation without high redundancy. Thus, it is split into two tables:
        1. bronze_customer_mgmt_event: Captures customer-level transactions (1 row per Action/Customer).
        2. bronze_customer_mgmt_account: Captures account-level states (1 row per nested Account),
           relationalized via foreign key (C_ID).
    - Staging-driven Bulk Ingestion. Data is staged to local CSV files using a memory-safe
      writer before issuing a high-throughput Snowflake `COPY INTO` command.
"""

import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Tuple

from ..common import compute_row_hash, StreamingCsvWriter
from ..snowflake_client import copy_into


def load_customer_mgmt_xml(
    conn: Any, 
    filepath: Path, 
    batch_id: int, 
    tmp_dir: Path
) -> Tuple[int, int]:
    """Parses `CustomerMgmt.xml`, flattens its hierarchy, and bulk-loads it into Snowflake Bronze tables.

    This function extracts customer actions and nested account entities, transforms string 
    representations into strongly-typed Python objects, appends lineage metadata columns 
    (`_batch_id`, `_source_file`, `_loaded_at`, `_row_hash`), and bulk-loads the records 
    via staged CSV files into two distinct Snowflake tables.

    Args:
        conn: Active Snowflake database connection or session object.
        filepath (Path): Local filesystem path to the `CustomerMgmt.xml` source file.
        batch_id (int): Unique identifier representing the current execution pipeline batch.
        tmp_dir (Path): Temporary directory path used to store intermediate staging CSV files.

    Returns:
        Tuple[int, int]: A tuple containing:
            - Number of rows inserted into `bronze_customer_mgmt_event`.
            - Number of rows inserted into `bronze_customer_mgmt_account`.

    Raises:
        ET.ParseError: If the source XML file is malformed or corrupted.
        OSError: If reading the file or writing to `tmp_dir` fails.
    """
    # 1. Audit Metadata Extraction
    source_file = filepath.name
    loaded_at = datetime.now(timezone.utc)

    # TPC-DI XML files use a default namespace (xmlns:TPCDI="http://www.tpc.org/tpc-di").
    NS_URI = "http://www.tpc.org/tpc-di"
    ACTION_TAG = f"{{{NS_URI}}}Action"

    event_path = tmp_dir / f"customer_mgmt_event_b{batch_id}.csv"
    account_path = tmp_dir / f"customer_mgmt_account_b{batch_id}.csv"

    # 2/3. Streaming DOM parse + flatten: iterparse fires "end" events as each
    # element closes, and elem.clear() drops that element's children once
    # we're done with it, so memory stays roughly proportional to one Action
    # block rather than the whole document (CustomerMgmt.xml can be large).
    # Two writers stay open concurrently so events/accounts are flushed to
    # disk as they're produced instead of being buffered in Python lists.
    with StreamingCsvWriter(event_path) as event_w, \
         StreamingCsvWriter(account_path) as account_w:

        for _event, action in ET.iterparse(filepath, events=("end",)):
            if action.tag != ACTION_TAG:
                continue

            action_type = action.get("ActionType")
            # Standardize timestamp ISO string to naive Python datetime object
            action_ts = datetime.strptime(action.get("ActionTS"), "%Y-%m-%dT%H:%M:%S")

            customer = action.find("Customer")
            if customer is None:
                # Guard clause: skip malformed action blocks missing customer details
                action.clear()
                continue

            # Extract mandatory & optional Customer attributes
            c_id = int(customer.get("C_ID"))
            c_tax_id = customer.get("C_TAX_ID")
            c_gndr = customer.get("C_GNDR")
            c_tier_raw = customer.get("C_TIER")
            c_dob_raw = customer.get("C_DOB")

            # Extract nested TaxInfo child node if available
            tax_info = customer.find("TaxInfo")
            c_lcl_tx_id = tax_info.findtext("C_LCL_TX_ID") if tax_info is not None else None
            c_nat_tx_id = tax_info.findtext("C_NAT_TX_ID") if tax_info is not None else None

            # Build Customer Event Business Record with cast data types
            event_values = [
                action_type,
                action_ts,
                c_id,
                c_tax_id,
                c_gndr,
                int(c_tier_raw) if c_tier_raw else None,
                datetime.strptime(c_dob_raw, "%Y-%m-%d").date() if c_dob_raw else None,
                c_lcl_tx_id,
                c_nat_tx_id,
            ]

            # Calculate row hash across business values for CDC/deduplication support
            row_hash = compute_row_hash(event_values)
            event_w.write(event_values + [batch_id, source_file, loaded_at, row_hash])

            # Extract 1-to-N nested <Account> entities bound to parent Customer ID (c_id)
            for acct in customer.findall("Account"):
                ca_id = int(acct.get("CA_ID"))
                ca_tax_st_raw = acct.get("CA_TAX_ST")
                ca_b_id_raw = acct.findtext("CA_B_ID")
                ca_name = acct.findtext("CA_NAME")

                acct_values = [
                    action_ts,
                    c_id,  # Foreign key linkage to parent customer
                    ca_id,
                    int(ca_tax_st_raw) if ca_tax_st_raw else None,
                    int(ca_b_id_raw) if ca_b_id_raw else None,
                    ca_name,
                ]

                acct_row_hash = compute_row_hash(acct_values)
                account_w.write(acct_values + [batch_id, source_file, loaded_at, acct_row_hash])

            # Free this Action's subtree now that it's been flattened to CSV.
            action.clear()

    total = 0

    # 4. Loading — Customer Event Table
    if event_w.count:
        cols = [
            "ActionType", "ActionTS", "C_ID", "C_TAX_ID", "C_GNDR", "C_TIER", "C_DOB",
            "C_LCL_TX_ID", "C_NAT_TX_ID", "_batch_id", "_source_file", "_loaded_at", "_row_hash"
        ]
        total += copy_into(conn, "bronze_customer_mgmt_event", cols, event_path)

    # 5. Loading — Customer Account Table
    if account_w.count:
        cols = [
            "ActionTS", "C_ID", "CA_ID", "CA_TAX_ST", "CA_B_ID", "CA_NAME",
            "_batch_id", "_source_file", "_loaded_at", "_row_hash"
        ]
        total += copy_into(conn, "bronze_customer_mgmt_account", cols, account_path)

    # Return row execution counts for data quality and pipeline metric logging
    return event_w.count, account_w.count

if __name__ == "__main__":
    # Example usage for local testing
    from pathlib import Path
    from ingestion.snowflake_client import get_snowflake_connection

    conn = get_snowflake_connection()
    batch_id = 1
    tmp_dir = Path("/tmp")
    xml_path = Path("data/Batch1/CustomerMgmt.xml")