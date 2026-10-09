# dbt project: crypto_lakehouse (DuckDB over the gold Delta tables)

```powershell
cd dbt
$env:LAKEHOUSE_GOLD = (Resolve-Path ..\data\gold).Path -replace '\\','/'
dbt build --profiles-dir .            # run all models + all tests
dbt docs generate --profiles-dir .    # lineage graph + column docs
dbt docs serve --profiles-dir . --port 8081
```

Layers: `staging` (views over gold) → `intermediate` (daily OHLCV, returns) →
`marts` (facts, scorecards, correlations) + `quality` (completeness checks).
`fct_daily_prices` and `mart_coin_performance` have **enforced contracts**.
