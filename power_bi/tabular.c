var existingRelationships = Model.Relationships.ToList();
foreach (var rel in existingRelationships)
{
    rel.Delete();
}

var relationships = new[]
{
    new { FromTable = "dim_account", FromCol = "broker_sk", ToTable = "dim_broker", ToCol = "broker_sk", Active = true },
    new { FromTable = "dim_account", FromCol = "customer_sk", ToTable = "dim_customer", ToCol = "customer_sk", Active = true },
    new { FromTable = "dim_security", FromCol = "company_sk", ToTable = "dim_company", ToCol = "company_sk", Active = true },

    new { FromTable = "fact_cashtransaction", FromCol = "transaction_date_sk", ToTable = "dim_date", ToCol = "date_sk", Active = true },
    new { FromTable = "fact_cashtransaction", FromCol = "transaction_time_sk", ToTable = "dim_time", ToCol = "time_sk", Active = true },
    new { FromTable = "fact_cashtransaction", FromCol = "account_sk", ToTable = "dim_account", ToCol = "account_sk", Active = true },

    new { FromTable = "fact_company_financials", FromCol = "company_sk", ToTable = "dim_company", ToCol = "company_sk", Active = true },
    new { FromTable = "fact_company_financials", FromCol = "fiscal_date_sk", ToTable = "dim_date", ToCol = "date_sk", Active = true },
    new { FromTable = "fact_company_financials", FromCol = "posting_date_sk", ToTable = "dim_date", ToCol = "date_sk", Active = false },

    new { FromTable = "fact_holding", FromCol = "originating_trade_id", ToTable = "fact_trade", ToCol = "trade_id", Active = false },
    new { FromTable = "fact_holding", FromCol = "current_trade_id", ToTable = "fact_trade", ToCol = "trade_id", Active = false },
    new { FromTable = "fact_holding", FromCol = "security_sk", ToTable = "dim_security", ToCol = "security_sk", Active = true },
    new { FromTable = "fact_holding", FromCol = "holding_date_sk", ToTable = "dim_date", ToCol = "date_sk", Active = true },
    new { FromTable = "fact_holding", FromCol = "account_sk", ToTable = "dim_account", ToCol = "account_sk", Active = true },

    new { FromTable = "fact_market_history", FromCol = "security_sk", ToTable = "dim_security", ToCol = "security_sk", Active = true },
    new { FromTable = "fact_market_history", FromCol = "market_date_sk", ToTable = "dim_date", ToCol = "date_sk", Active = true },

    new { FromTable = "fact_trade", FromCol = "security_sk", ToTable = "dim_security", ToCol = "security_sk", Active = true },
    new { FromTable = "fact_trade", FromCol = "trade_date_sk", ToTable = "dim_date", ToCol = "date_sk", Active = true },
    new { FromTable = "fact_trade", FromCol = "trade_time_sk", ToTable = "dim_time", ToCol = "time_sk", Active = true },
    new { FromTable = "fact_trade", FromCol = "status_type_sk", ToTable = "dim_statustype", ToCol = "status_type_sk", Active = true },
    new { FromTable = "fact_trade", FromCol = "trade_type_sk", ToTable = "dim_tradetype", ToCol = "trade_type_sk", Active = true },
    new { FromTable = "fact_trade", FromCol = "account_sk", ToTable = "dim_account", ToCol = "account_sk", Active = true },

    new { FromTable = "fact_trade_history", FromCol = "trade_sk", ToTable = "fact_trade", ToCol = "trade_sk", Active = false },
    new { FromTable = "fact_trade_history", FromCol = "account_sk", ToTable = "dim_account", ToCol = "account_sk", Active = true },
    new { FromTable = "fact_trade_history", FromCol = "security_sk", ToTable = "dim_security", ToCol = "security_sk", Active = true },
    new { FromTable = "fact_trade_history", FromCol = "status_type_sk", ToTable = "dim_statustype", ToCol = "status_type_sk", Active = true },
    new { FromTable = "fact_trade_history", FromCol = "status_date_sk", ToTable = "dim_date", ToCol = "date_sk", Active = true },
    new { FromTable = "fact_trade_history", FromCol = "status_time_sk", ToTable = "dim_time", ToCol = "time_sk", Active = true },

    new { FromTable = "fact_watchitem", FromCol = "customer_sk", ToTable = "dim_customer", ToCol = "customer_sk", Active = true },
    new { FromTable = "fact_watchitem", FromCol = "security_sk", ToTable = "dim_security", ToCol = "security_sk", Active = true },
    new { FromTable = "fact_watchitem", FromCol = "watch_date_sk", ToTable = "dim_date", ToCol = "date_sk", Active = true },
    new { FromTable = "fact_watchitem", FromCol = "watch_time_sk", ToTable = "dim_time", ToCol = "time_sk", Active = true }
};

foreach (var r in relationships)
{
    var fromCol = Model.Tables[r.FromTable].Columns[r.FromCol];
    var toCol = Model.Tables[r.ToTable].Columns[r.ToCol];

    var rel = Model.AddRelationship();
    rel.FromColumn = fromCol;
    rel.ToColumn = toCol;
    rel.IsActive = r.Active;
    rel.CrossFilteringBehavior = CrossFilteringBehavior.OneDirection;
}