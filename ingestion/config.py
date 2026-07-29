"""
Declarative configuration for every delimited (pipe- or comma-separated)
source. This is the single place that describes each source's column
layout and target table — the loader itself is generic (see
loaders/delimited_loader.py) and reads this config to know what to do.

Each entry:
  filename        - expected filename inside a batch directory
  delimiter       - field delimiter
  target_table    - Snowflake bronze table this source lands in
  cdc_capable     - whether this source's schema includes CDC_FLAG/CDC_DSN
                    in *some* batches (Batch1 vs Batch2/3 divergence).
                    The loader auto-detects, per line, whether CDC columns
                    are actually present by comparing field counts.
  columns         - ordered list of (business_column_name, caster) tuples,
                    matching the file's column order EXCLUDING any
                    CDC_FLAG/CDC_DSN prefix.
"""
from .common import parse_date, parse_datetime, parse_bool, parse_decimal



DELIMITED_SOURCES = {
    # ---- Archetype A: static reference dimensions --------------------------
    "date": {
        "filename": "Date.txt",
        "delimiter": "|",
        "target_table": "bronze_date",
        "cdc_capable": False,
        "columns": [
            ("sk_dateid", int),
            ("datevalue", parse_date),
            ("datedesc", str),
            ("calendaryearid", int),
            ("calendaryeardesc", str),
            ("calendarqtrid", int),
            ("calendarqtrdesc", str),
            ("calendarmonthid", int),
            ("calendarmonthdesc", str),
            ("calendarweekid", int),
            ("calendarweekdesc", str),
            ("dayofweeknum", int),
            ("dayofweekdesc", str),
            ("fiscalyearid", int),
            ("fiscalyeardesc", str),
            ("fiscalqtrid", int),
            ("fiscalqtrdesc", str),
            ("holidayflag", parse_bool),
        ],
    },
    "time": {
        "filename": "Time.txt",
        "delimiter": "|",
        "target_table": "bronze_time",
        "cdc_capable": False,
        "columns": [
            ("sk_timeid", int),
            ("timevalue", str),
            ("hourid", int),
            ("hourdesc", str),
            ("minuteid", int),
            ("minutedesc", str),
            ("secondid", int),
            ("seconddesc", str),
            ("markethoursflag", parse_bool),
            ("officehoursflag", parse_bool),
        ],
    },
    "status_type": {
        "filename": "StatusType.txt",
        "delimiter": "|",
        "target_table": "bronze_status_type",
        "cdc_capable": False,
        "columns": [
            ("st_id", str),
            ("st_name", str),
        ],
    },
    "tax_rate": {
        "filename": "TaxRate.txt",
        "delimiter": "|",
        "target_table": "bronze_tax_rate",
        "cdc_capable": False,
        "columns": [
            ("tx_id", str),
            ("tx_name", str),
            ("tx_rate", parse_decimal),
        ],
    },
    "industry": {
        "filename": "Industry.txt",
        "delimiter": "|",
        "target_table": "bronze_industry",
        "cdc_capable": False,
        "columns": [
            ("in_id", str),
            ("in_name", str),
            ("in_sc_id", str),
        ],
    },
    "trade_type": {
        "filename": "TradeType.txt",
        "delimiter": "|",
        "target_table": "bronze_trade_type",
        "cdc_capable": False,
        "columns": [
            ("tt_id", str),
            ("tt_name", str),
            ("tt_is_sell", int),  # Kept as int (NUMBER(1,0)) per DDL
            ("tt_is_mrkt", int),  # Kept as int (NUMBER(1,0)) per DDL
        ],
    },
    "hr": {
        "filename": "HR.csv",
        "delimiter": ",",
        "target_table": "bronze_hr",
        "cdc_capable": False,
        "columns": [
            ("employeeid", int),
            ("managerid", int),
            ("employeefirstname", str),
            ("employeelastname", str),
            ("employeemi", str),
            ("employeejobcode", int),
            ("employeebranch", str),
            ("employeeoffice", str),
            ("employeephone", str),
        ],
    },

    # ---- Archetype C: full re-extract snapshot ------------------------------
    "prospect": {
        "filename": "Prospect.csv",
        "delimiter": ",",
        "target_table": "bronze_prospect",
        "cdc_capable": False,
        "columns": [
            ("agencyid", str),
            ("lastname", str),
            ("firstname", str),
            ("middleinitial", str),
            ("gender", str),
            ("addressline1", str),
            ("addressline2", str),
            ("postalcode", str),
            ("city", str),
            ("state", str),
            ("country", str),
            ("phone", str),
            ("income", int),
            ("numbercars", int),
            ("numberchildren", int),
            ("maritalstatus", str),
            ("age", int),
            ("creditrating", int),
            ("ownorrentflag", str),
            ("employer", str),
            ("numbercreditcards", int),
            ("networth", int),
        ],
    },

    # ---- Archetype B: schema-shifting CDC facts -----------------------------
    "account": {
        "filename": "Account.txt",
        "delimiter": "|",
        "target_table": "bronze_account",
        "cdc_capable": True,
        "columns": [
            ("ca_id", int),
            ("ca_b_id", int),
            ("ca_c_id", int),
            ("ca_name", str),
            ("ca_tax_st", int),
            ("ca_st_id", str),
        ],
    },
    "customer": {
        "filename": "Customer.txt",
        "delimiter": "|",
        "target_table": "bronze_customer",
        "cdc_capable": True,
        "columns": [
            ("c_id", int),
            ("c_tax_id", str),
            ("c_st_id", str),
            ("c_l_name", str),
            ("c_f_name", str),
            ("c_m_name", str),
            ("c_gndr", str),
            ("c_tier", int),
            ("c_dob", parse_date),
            ("c_adline1", str),
            ("c_adline2", str),
            ("c_zipcode", str),
            ("c_city", str),
            ("c_state_prov", str),
            ("c_ctry", str),
            ("c_ctry_1", str),
            ("c_area_1", str),
            ("c_local_1", str),
            ("c_ext_1", str),
            ("c_ctry_2", str),
            ("c_area_2", str),
            ("c_local_2", str),
            ("c_ext_2", str),
            ("c_ctry_3", str),
            ("c_area_3", str),
            ("c_local_3", str),
            ("c_ext_3", str),
            ("c_prim_email", str),
            ("c_alt_email", str),
            ("c_lcl_tx_id", str),
            ("c_nat_tx_id", str),
        ],
    },
    "trade": {
        "filename": "Trade.txt",
        "delimiter": "|",
        "target_table": "bronze_trade",
        "cdc_capable": True,
        "columns": [
            ("t_id", int),
            ("t_dts", parse_datetime),
            ("t_st_id", str),
            ("t_tt_id", str),
            ("t_is_cash", parse_bool),
            ("t_s_symb", str),
            ("t_qty", int),
            ("t_bid_price", parse_decimal),
            ("t_ca_id", int),
            ("t_exec_name", str),
            ("t_trade_price", parse_decimal),
            ("t_chrg", parse_decimal),
            ("t_comm", parse_decimal),
            ("t_tax", parse_decimal ),
        ],
    },
    "holding_history": {
        "filename": "HoldingHistory.txt",
        "delimiter": "|",
        "target_table": "bronze_holding_history",
        "cdc_capable": True,
        "columns": [
            ("hh_h_t_id", int),
            ("hh_t_id", int),
            ("hh_before_qty", int),
            ("hh_after_qty", int),
        ],
    },
    "watch_history": {
        "filename": "WatchHistory.txt",
        "delimiter": "|",
        "target_table": "bronze_watch_history",
        "cdc_capable": True,
        "columns": [
            ("w_c_id", int),
            ("w_s_symb", str),
            ("w_dts", parse_datetime),
            ("w_action", str),
        ],
    },
    "daily_market": {
        "filename": "DailyMarket.txt",
        "delimiter": "|",
        "target_table": "bronze_daily_market",
        "cdc_capable": True,
        "columns": [
            ("dm_date", parse_date),
            ("dm_s_symb", str),
            ("dm_close", float),
            ("dm_high", float),
            ("dm_low", float),
            ("dm_vol", int),
        ],
    },
    "cash_transaction": {
        "filename": "CashTransaction.txt",
        "delimiter": "|",
        "target_table": "bronze_cash_transaction",
        "cdc_capable": True,
        "columns": [
            ("ct_ca_id", int),
            ("ct_dts", parse_datetime),
            ("ct_amt", parse_decimal),
            ("ct_name", str),
        ],
    },

    # ---- Archetype E: Historical-load-only fact -----------------------------
    "trade_history": {
        "filename": "TradeHistory.txt",
        "delimiter": "|",
        "target_table": "bronze_trade_history",
        "cdc_capable": False,
        "columns": [
            ("th_t_id", int),
            ("th_dts", parse_datetime),
            ("th_st_id", str),
        ],
    },
}