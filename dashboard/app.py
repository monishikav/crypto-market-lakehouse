"""Crypto Market Lakehouse - dashboard over the dbt marts (DuckDB).

    streamlit run dashboard/app.py

Reads data/crypto_lakehouse.duckdb in read-only mode (run `dbt build` first), plus the
streaming candles table if the real-time lane has run. Don't run dbt while this is open:
DuckDB allows one writer, and dbt needs to be it.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
DB = Path(os.getenv("DBT_DUCKDB_PATH", ROOT / "data" / "crypto_lakehouse.duckdb"))
STREAM = ROOT / "data" / "silver" / "stream_ohlcv_1m"

st.set_page_config(page_title="Crypto Market Lakehouse", page_icon="📈", layout="wide")


@st.cache_resource
def conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(str(DB), read_only=True)
    c.sql("SET TimeZone = 'UTC'")
    return c


@st.cache_data(ttl=300)
def q(sql: str) -> pd.DataFrame:
    return conn().sql(sql).df()


if not DB.exists():
    st.error(f"No warehouse at {DB}. Run `dbt build --profiles-dir .` in the dbt folder first.")
    st.stop()

st.title("📈 Crypto Market Lakehouse")
span = q("select min(price_date) as lo, max(price_date) as hi, count(distinct symbol) as n "
         "from marts.fct_daily_prices").iloc[0]  # fmt: skip
st.caption(
    f"{int(span.n)} coins · {span.lo:%d %b %Y} → {span.hi:%d %b %Y} · "
    "Binance 1-minute candles → Spark/Delta → dbt marts on DuckDB"
)

perf = q("select * from marts.mart_coin_performance order by return_rank")
dq = q(
    "select count(*) as days, sum(missing_minutes) as missing from quality.dq_daily_completeness"
)
best, worst = perf.iloc[0], perf.iloc[-1]

k = st.columns(5)
k[0].metric("Coins tracked", len(perf))
k[1].metric("Best performer", best.symbol.replace("USDT", ""), f"{best.period_return:+.1%}")
k[2].metric("Worst performer", worst.symbol.replace("USDT", ""), f"{worst.period_return:+.1%}")
k[3].metric("Avg daily volume", f"${perf.avg_daily_quote_volume_usdt.sum() / 1e9:,.1f}B")
k[4].metric(
    "Missing minutes", f"{int(dq.missing.iloc[0]):,}", f"{int(dq.days.iloc[0]):,} coin-days"
)

tab_overview, tab_coin, tab_patterns, tab_live = st.tabs(
    ["Scorecard", "Coin explorer", "Market patterns", "Real-time lane"]
)

with tab_overview:
    st.subheader("Coin scorecard")
    st.dataframe(
        perf[
            [
                "return_rank",
                "symbol",
                "coin_name",
                "period_return",
                "annualized_volatility",
                "sharpe_ratio",
                "max_drawdown",
                "best_day",
                "best_day_return",
                "worst_day",
                "worst_day_return",
                "pct_up_days",
            ]
        ],  # fmt: skip
        hide_index=True,
        width="stretch",
        column_config={
            "return_rank": "Rank",
            "symbol": "Symbol",
            "coin_name": "Coin",
            "best_day": st.column_config.DateColumn("Best day", format="DD MMM YYYY"),
            "worst_day": st.column_config.DateColumn("Worst day", format="DD MMM YYYY"),
            "period_return": st.column_config.NumberColumn("Return", format="percent"),
            "annualized_volatility": st.column_config.NumberColumn("Volatility", format="percent"),
            "sharpe_ratio": st.column_config.NumberColumn("Sharpe", format="%.2f"),
            "max_drawdown": st.column_config.NumberColumn("Max drawdown", format="percent"),
            "best_day_return": st.column_config.NumberColumn("Best day %", format="percent"),
            "worst_day_return": st.column_config.NumberColumn("Worst day %", format="percent"),
            "pct_up_days": st.column_config.NumberColumn("Up days", format="percent"),
        },
    )
    st.subheader("Return over the period")
    st.bar_chart(perf.set_index("symbol")["period_return"], horizontal=True)

    st.subheader("Monthly returns")
    monthly = q("select symbol, strftime(month_start, '%Y-%m') as month, monthly_return "
                "from marts.fct_monthly_returns")  # fmt: skip
    grid = monthly.pivot(index="symbol", columns="month", values="monthly_return")
    st.dataframe(
        grid.style.format("{:+.1%}").map(
            lambda v: f"color: {'#1a7f37' if v > 0 else '#cf222e'}" if pd.notna(v) else ""
        ),
        width="stretch",
    )

with tab_coin:
    symbol = st.selectbox("Coin", perf.symbol.tolist())
    daily = q(f"""select price_date, close_price::double as close, sma_7d, sma_30d,
                         volatility_30d_annualized, drawdown_from_peak,
                         quote_volume_usdt::double as quote_volume
                  from marts.fct_daily_prices where symbol = '{symbol}' order by price_date""")
    daily = daily.set_index("price_date")
    st.subheader(f"{symbol}: close with 7- and 30-day moving averages")
    st.line_chart(daily[["close", "sma_7d", "sma_30d"]])
    c1, c2 = st.columns(2)
    c1.subheader("30-day annualized volatility")
    c1.line_chart(daily["volatility_30d_annualized"])
    c2.subheader("Drawdown from running peak")
    c2.area_chart(daily["drawdown_from_peak"])
    st.subheader("Daily traded volume (USDT)")
    st.bar_chart(daily["quote_volume"])

with tab_patterns:
    c1, c2 = st.columns(2)
    corr = q("select symbol, correlation_with_btc, beta_to_btc from marts.mart_btc_correlation "
             "where symbol <> 'BTCUSDT' order by correlation_with_btc desc")  # fmt: skip
    c1.subheader("Correlation with Bitcoin")
    c1.bar_chart(corr.set_index("symbol")["correlation_with_btc"], horizontal=True)
    c2.subheader("Beta to Bitcoin (sensitivity)")
    c2.bar_chart(corr.set_index("symbol")["beta_to_btc"], horizontal=True)

    hourly = q("select hour_ist, avg(share_of_daily_volume) as share "
               "from marts.mart_hourly_volume_profile group by 1 order by 1")  # fmt: skip
    st.subheader("When does the market trade? Share of daily volume by hour (IST)")
    st.bar_chart(hourly.set_index("hour_ist")["share"])

    weekday = q("select day_of_week, any_value(day_name) as day, avg(avg_daily_return) as ret "
                "from marts.mart_weekday_seasonality group by 1 order by 1")  # fmt: skip
    st.subheader("Average daily return by weekday (all coins)")
    st.bar_chart(weekday.set_index("day")["ret"], sort=False)

with tab_live:
    if not (STREAM / "_delta_log").exists():
        st.info("No streaming data yet. Run the producer + `trades_to_delta` job (see README).")
    else:
        live = (
            conn()
            .sql(f"select * from delta_scan('{STREAM.as_posix()}') order by symbol, window_start")
            .df()
        )
        st.caption(
            f"{len(live)} one-minute candles built by Spark Structured Streaming from "
            f"{int(live.number_of_trades.sum()):,} live trades"
        )
        sym = st.selectbox("Symbol", sorted(live.symbol.unique()))
        one = live[live.symbol == sym].set_index("window_start")
        st.line_chart(one[["open", "high", "low", "close"]].astype(float))
        st.bar_chart(one["number_of_trades"])
        st.dataframe(one.reset_index(), hide_index=True, width="stretch")
