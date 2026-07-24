"""
Declarative configuration for every delimited (pipe- or comma-separated)
source. This is the single place that describes each source's column
layout and target table — the loader itself is generic (see
loaders/delimited_loader.py) and reads this config to know what to do.

Each entry:
  filename       - expected filename inside a batch directory
  delimiter      - field delimiter
  target_table   - Snowflake bronze table this source lands in
  cdc_capable    - whether this source's schema includes CDC_FLAG/CDC_DSN
                    in *some* batches (Batch1 vs Batch2/3 divergence).
                    The loader auto-detects, per line, whether CDC columns
                    are actually present by comparing field counts.
  columns        - ordered list of (business_column_name, caster) tuples,
                    matching the file's column order EXCLUDING any
                    CDC_FLAG/CDC_DSN prefix.
"""
from decimal import Decimal
from .common import parse_date, parse_datetime, parse_bool


def dec(value: str) -> Decimal:
    return Decimal(value)


DELIMITED_SOURCES = {
    # ---- Archetype A: static reference dimensions --------------------------
    "date": {
        "filename": "Date.txt",
        "delimiter": "|",
        "target_table": "bronze_date",
        "cdc_capable": False,
        "columns": [
            ("SK_DateID", int),
            ("DateValue", parse_date),
            ("DateDesc", str),
            ("CalendarYearID", int),
            ("CalendarYearDesc", str),
            ("CalendarQtrID", int),
            ("CalendarQtrDesc", str),
            ("CalendarMonthID", int),
            ("CalendarMonthDesc", str),
            ("CalendarWeekID", int),
            ("CalendarWeekDesc", str),
            ("DayOfWeekNum", int),
            ("DayOfWeekDesc", str),
            ("FiscalYearID", int),
            ("FiscalYearDesc", str),
            ("FiscalQtrID", int),
            ("FiscalQtrDesc", str),
            ("HolidayFlag", parse_bool),
        ],
    },
    "time": {
        "filename": "Time.txt",
        "delimiter": "|",
        "target_table": "bronze_time",
        "cdc_capable": False,
        "columns": [
            ("SK_TimeID", int),
            ("TimeValue", str),
            ("HourID", int),
            ("HourDesc", str),
            ("MinuteID", int),
            ("MinuteDesc", str),
            ("SecondID", int),
            ("SecondDesc", str),
            ("MarketHoursFlag", parse_bool),
            ("OfficeHoursFlag", parse_bool),
        ],
    },
    "status_type": {
        "filename": "StatusType.txt",
        "delimiter": "|",
        "target_table": "bronze_status_type",
        "cdc_capable": False,
        "columns": [
            ("ST_ID", str),
            ("ST_NAME", str),
        ],
    },
    "tax_rate": {
        "filename": "TaxRate.txt",
        "delimiter": "|",
        "target_table": "bronze_tax_rate",
        "cdc_capable": False,
        "columns": [
            ("TX_ID", str),
            ("TX_NAME", str),
            ("TX_RATE", dec),
        ],
    },
    "industry": {
        "filename": "Industry.txt",
        "delimiter": "|",
        "target_table": "bronze_industry",
        "cdc_capable": False,
        "columns": [
            ("IN_ID", str),
            ("IN_NAME", str),
            ("IN_SC_ID", str),
        ],
    },
    "trade_type": {
        "filename": "TradeType.txt",
        "delimiter": "|",
        "target_table": "bronze_trade_type",
        "cdc_capable": False,
        "columns": [
            ("TT_ID", str),
            ("TT_NAME", str),
            ("TT_IS_SELL", parse_bool),
            ("TT_IS_MRKT", parse_bool),
        ],
    },
    "hr": {
        "filename": "HR.csv",
        "delimiter": ",",
        "target_table": "bronze_hr",
        "cdc_capable": False,
        "columns": [
            ("EmployeeID", int),
            ("ManagerID", int),
            ("EmployeeFirstName", str),
            ("EmployeeLastName", str),
            ("EmployeeMI", str),
            ("EmployeeJobCode", int),
            ("EmployeeBranch", str),
            ("EmployeeOffice", str),
            ("EmployeePhone", str),
        ],
    },

    # ---- Archetype C: full re-extract snapshot ------------------------------
    "prospect": {
        "filename": "Prospect.csv",
        "delimiter": ",",
        "target_table": "bronze_prospect",
        "cdc_capable": False,
        "columns": [
            ("AgencyID", str),
            ("LastName", str),
            ("FirstName", str),
            ("MiddleInitial", str),
            ("Gender", str),
            ("AddressLine1", str),
            ("AddressLine2", str),
            ("PostalCode", str),
            ("City", str),
            ("State", str),
            ("Country", str),
            ("Phone", str),
            ("Income", int),
            ("NumberCars", int),
            ("NumberChildren", int),
            ("MaritalStatus", str),
            ("Age", int),
            ("CreditRating", int),
            ("OwnOrRentFlag", str),
            ("Employer", str),
            ("NumberCreditCards", int),
            ("NetWorth", int),
        ],
    },

    # ---- Archetype B: schema-shifting CDC facts -----------------------------
    "account": {
        "filename": "Account.txt",
        "delimiter": "|",
        "target_table": "bronze_account",
        "cdc_capable": True,
        "columns": [
            ("CA_ID", int),
            ("CA_B_ID", int),
            ("CA_C_ID", int),
            ("CA_NAME", str),
            ("CA_TAX_ST", int),
            ("CA_ST_ID", str),
        ],
    },
    "customer": {
        "filename": "Customer.txt",
        "delimiter": "|",
        "target_table": "bronze_customer",
        "cdc_capable": True,
        "columns": [
            ("C_ID", int),
            ("C_TAX_ID", str),
            ("C_ST_ID", str),
            ("C_L_NAME", str),
            ("C_F_NAME", str),
            ("C_M_NAME", str),
            ("C_GNDR", str),
            ("C_TIER", int),
            ("C_DOB", parse_date),
            ("C_ADLINE1", str),
            ("C_ADLINE2", str),
            ("C_ZIPCODE", str),
            ("C_CITY", str),
            ("C_STATE_PROV", str),
            ("C_CTRY", str),
            ("C_CTRY_1", str),
            ("C_AREA_1", str),
            ("C_LOCAL_1", str),
            ("C_EXT_1", str),
            ("C_CTRY_2", str),
            ("C_AREA_2", str),
            ("C_LOCAL_2", str),
            ("C_EXT_2", str),
            ("C_CTRY_3", str),
            ("C_AREA_3", str),
            ("C_LOCAL_3", str),
            ("C_EXT_3", str),
            ("C_PRIM_EMAIL", str),
            ("C_ALT_EMAIL", str),
            ("C_LCL_TX_ID", str),
            ("C_NAT_TX_ID", str),
        ],
    },
    "trade": {
        "filename": "Trade.txt",
        "delimiter": "|",
        "target_table": "bronze_trade",
        "cdc_capable": True,
        "columns": [
            ("T_ID", int),
            ("T_DTS", parse_datetime),
            ("T_ST_ID", str),
            ("T_TT_ID", str),
            ("T_IS_CASH", parse_bool),
            ("T_S_SYMB", str),
            ("T_QTY", int),
            ("T_BID_PRICE", dec),
            ("T_CA_ID", int),
            ("T_EXEC_NAME", str),
            ("T_TRADE_PRICE", dec),
            ("T_CHRG", dec),
            ("T_COMM", dec),
            ("T_TAX", dec),
        ],
    },
    "trade_history": {
        "filename": "TradeHistory.txt",
        "delimiter": "|",
        "target_table": "bronze_trade_history",
        "cdc_capable": False,
        "columns": [
            ("TH_T_ID", int),
            ("TH_DTS", parse_datetime),
            ("TH_ST_ID", str),
        ],
    },
    "holding_history": {
        "filename": "HoldingHistory.txt",
        "delimiter": "|",
        "target_table": "bronze_holding_history",
        "cdc_capable": True,
        "columns": [
            ("HH_H_T_ID", int),
            ("HH_T_ID", int),
            ("HH_BEFORE_QTY", int),
            ("HH_AFTER_QTY", int),
        ],
    },
    "watch_history": {
        "filename": "WatchHistory.txt",
        "delimiter": "|",
        "target_table": "bronze_watch_history",
        "cdc_capable": True,
        "columns": [
            ("W_C_ID", int),
            ("W_S_SYMB", str),
            ("W_DTS", parse_datetime),
            ("W_ACTION", str),
        ],
    },
    "daily_market": {
        "filename": "DailyMarket.txt",
        "delimiter": "|",
        "target_table": "bronze_daily_market",
        "cdc_capable": True,
        "columns": [
            ("DM_DATE", parse_date),
            ("DM_S_SYMB", str),
            ("DM_CLOSE", float),
            ("DM_HIGH", float),
            ("DM_LOW", float),
            ("DM_VOL", int),
        ],
    },
    "cash_transaction": {
        "filename": "CashTransaction.txt",
        "delimiter": "|",
        "target_table": "bronze_cash_transaction",
        "cdc_capable": True,
        "columns": [
            ("CT_CA_ID", int),
            ("CT_DTS", parse_datetime),
            ("CT_AMT", dec),
            ("CT_NAME", str),
        ],
    },
}