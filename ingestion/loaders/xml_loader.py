"""
Loader module for ingesting CustomerMgmt.xml into Snowflake Bronze Layer.

Architectural Design Records:
    - Flattening nested XML into relational tables. Nested XML structure
      (<Action> -> <Customer> -> <Account>) lacks a natural single-table tabular
      representation without high redundancy. Thus, it is split into two tables:
        1. bronze_mgmt_customer: Captures customer-level transactions (1 row per Action/Customer).
        2. bronze_mgmt_account: Captures account-level states (1 row per nested Account),
           relationalized via foreign key (C_ID).
    - Staging-driven Bulk Ingestion. Data is staged to local CSV files using a memory-safe
      writer before issuing a high-throughput Snowflake `COPY INTO` command.
    - Sparse payload: per spec (confirmed against a real UPDCUST sample --
      C_TIER genuinely absent on that action, contra the ambiguous "Not
      empty" wording in the spec's prose table; the XSD's minOccurs=0 is
      the authoritative source), only required properties are supplied per
      action. Every optional field below is read with .get()/findtext(),
      both of which return None when the attribute/element is absent, so a
      missing value naturally becomes NULL on COPY INTO. No coalescing or
      carry-forward happens here -- that is a Silver responsibility.
    - Phone numbers: per the CustomerMgmt XSD (PhoneNumber complex type),
      C_PHONE_1/2/3 are each their own nested element containing
      C_CTRY_CODE / C_AREA_CODE / C_LOCAL / C_EXT -- not flat attributes.
      Confirmed against a real sample (UPDCUST action, C_ID=0) showing
      exactly this structure. Column names mirror Customer.txt's flattened
      convention (c_ctry_1, c_area_1, ...) by decision, so the two sources
      share a naming convention ahead of any silver-layer unification.
"""

import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Tuple

from ..common import compute_row_hash, StreamingCsvWriter
from ..snowflake_client import copy_into


def load_customer_mgmt_xml(
    conn: Any,
    filepath: Path,
    batch_id: int,
    tmp_dir: Path
) -> Tuple[int, int]:
    """Parses `CustomerMgmt.xml`, flattens its hierarchy, and bulk-loads it into Snowflake Bronze tables.

    Returns:
        Tuple[int, int]: rows inserted into (bronze_mgmt_customer, bronze_mgmt_account).
    """
    source_file = filepath.name
    loaded_at = datetime.now(timezone.utc)

    NS_URI = "http://www.tpc.org/tpc-di"
    ACTION_TAG = f"{{{NS_URI}}}Action"

    customer_path = tmp_dir / f"mgmt_customer_b{batch_id}.csv"
    account_path = tmp_dir / f"mgmt_account_b{batch_id}.csv"

    def _int_or_none(raw: Optional[str]) -> Optional[int]:
        return int(raw) if raw else None

    def _phone_parts(contact_info, element_name: str):
        """
        Extract (ctry_code, area_code, local, ext) from a nested PhoneNumber
        element (e.g. <C_PHONE_1><C_CTRY_CODE/>...</C_PHONE_1>). Returns
        four Nones if contact_info is absent or the phone element itself
        is absent -- both are legitimate per the XSD (minOccurs="0" at
        every level: ContactInfo, C_PHONE_N, and each part inside it).
        """
        if contact_info is None:
            return None, None, None, None
        phone_el = contact_info.find(element_name)
        if phone_el is None:
            return None, None, None, None
        return (
            phone_el.findtext("C_CTRY_CODE"),
            phone_el.findtext("C_AREA_CODE"),
            phone_el.findtext("C_LOCAL"),
            phone_el.findtext("C_EXT"),
        )

    with StreamingCsvWriter(customer_path) as customer_w, \
         StreamingCsvWriter(account_path) as account_w:

        for _event, action in ET.iterparse(filepath, events=("end",)):
            if action.tag != ACTION_TAG:
                continue

            action_type = action.get("ActionType")
            action_ts = datetime.strptime(action.get("ActionTS"), "%Y-%m-%dT%H:%M:%S")

            customer = action.find("Customer")
            if customer is None:
                action.clear()
                continue

            c_id = int(customer.get("C_ID"))
            c_tax_id = customer.get("C_TAX_ID")
            c_gndr = customer.get("C_GNDR")
            c_tier_raw = customer.get("C_TIER")
            c_dob_raw = customer.get("C_DOB")

            # Confirmed against real sample + XSD: Name/Address/ContactInfo/
            # TaxInfo are nested elements under Customer, each optional.
            name = customer.find("Name")
            c_l_name = name.findtext("C_L_NAME") if name is not None else None
            c_f_name = name.findtext("C_F_NAME") if name is not None else None
            c_m_name = name.findtext("C_M_NAME") if name is not None else None

            address = customer.find("Address")
            c_adline1 = address.findtext("C_ADLINE1") if address is not None else None
            c_adline2 = address.findtext("C_ADLINE2") if address is not None else None
            c_zipcode = address.findtext("C_ZIPCODE") if address is not None else None
            c_city = address.findtext("C_CITY") if address is not None else None
            c_state_prov = address.findtext("C_STATE_PROV") if address is not None else None
            c_ctry = address.findtext("C_CTRY") if address is not None else None

            contact_info = customer.find("ContactInfo")
            c_prim_email = contact_info.findtext("C_PRIM_EMAIL") if contact_info is not None else None
            c_alt_email = contact_info.findtext("C_ALT_EMAIL") if contact_info is not None else None

            c_ctry_1, c_area_1, c_local_1, c_ext_1 = _phone_parts(contact_info, "C_PHONE_1")
            c_ctry_2, c_area_2, c_local_2, c_ext_2 = _phone_parts(contact_info, "C_PHONE_2")
            c_ctry_3, c_area_3, c_local_3, c_ext_3 = _phone_parts(contact_info, "C_PHONE_3")

            tax_info = customer.find("TaxInfo")
            c_lcl_tx_id = tax_info.findtext("C_LCL_TX_ID") if tax_info is not None else None
            c_nat_tx_id = tax_info.findtext("C_NAT_TX_ID") if tax_info is not None else None

            # Order matches bronze_mgmt_customer DDL exactly.
            customer_values = [
                action_type,
                action_ts,
                c_id,
                c_tax_id,
                c_gndr,
                _int_or_none(c_tier_raw),
                datetime.strptime(c_dob_raw, "%Y-%m-%d").date() if c_dob_raw else None,
                c_l_name,
                c_f_name,
                c_m_name,
                c_adline1,
                c_adline2,
                c_zipcode,
                c_city,
                c_state_prov,
                c_ctry,
                c_prim_email,
                c_alt_email,
                c_ctry_1, c_area_1, c_local_1, c_ext_1,
                c_ctry_2, c_area_2, c_local_2, c_ext_2,
                c_ctry_3, c_area_3, c_local_3, c_ext_3,
                c_lcl_tx_id,
                c_nat_tx_id,
            ]

            row_hash = compute_row_hash(customer_values)
            customer_w.write(customer_values + [batch_id, source_file, loaded_at, row_hash])

            for acct in customer.findall("Account"):
                ca_id_raw = acct.get("CA_ID")
                ca_tax_st_raw = acct.get("CA_TAX_ST")
                ca_b_id_raw = acct.findtext("CA_B_ID")
                ca_name = acct.findtext("CA_NAME")

                # Order matches bronze_mgmt_account DDL exactly (actiontype
                # included -- CLOSEACCT/INACT status must be derivable from
                # this column downstream, per the DDL note).
                acct_values = [
                    action_type,
                    action_ts,
                    c_id,
                    _int_or_none(ca_id_raw),
                    _int_or_none(ca_tax_st_raw),
                    _int_or_none(ca_b_id_raw),
                    ca_name,
                ]

                acct_row_hash = compute_row_hash(acct_values)
                account_w.write(acct_values + [batch_id, source_file, loaded_at, acct_row_hash])

            action.clear()

    if customer_w.count:
        cols = [
            "actiontype", "actionts", "c_id", "c_tax_id", "c_gndr", "c_tier", "c_dob",
            "c_l_name", "c_f_name", "c_m_name", "c_adline1", "c_adline2", "c_zipcode",
            "c_city", "c_state_prov", "c_ctry", "c_prim_email", "c_alt_email",
            "c_ctry_1", "c_area_1", "c_local_1", "c_ext_1",
            "c_ctry_2", "c_area_2", "c_local_2", "c_ext_2",
            "c_ctry_3", "c_area_3", "c_local_3", "c_ext_3",
            "c_lcl_tx_id", "c_nat_tx_id",
            "_batch_id", "_source_file", "_loaded_at", "_row_hash",
        ]
        copy_into(conn, "bronze_mgmt_customer", cols, customer_path)

    if account_w.count:
        cols = [
            "actiontype", "actionts", "c_id", "ca_id", "ca_tax_st", "ca_b_id", "ca_name",
            "_batch_id", "_source_file", "_loaded_at", "_row_hash",
        ]
        copy_into(conn, "bronze_mgmt_account", cols, account_path)

    return customer_w.count, account_w.count