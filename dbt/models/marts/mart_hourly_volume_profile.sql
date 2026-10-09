-- When during the day does trading happen? Share of each coin's volume by UTC hour
-- (add 5:30 for IST). Built straight from minute data - DuckDB scans 10M rows in seconds.
with hourly as (
    select
        coin_key,
        hour(open_time_utc)                     as hour_utc,
        sum(quote_volume_usdt::double)          as quote_volume_usdt,
        sum(number_of_trades)                   as trades
    from {{ ref('stg_prices_minute') }}
    group by all
)

select
    h.coin_key,
    c.symbol,
    h.hour_utc,
    (h.hour_utc * 60 + 330) % 1440 // 60       as hour_ist,
    h.quote_volume_usdt,
    h.trades,
    h.quote_volume_usdt / sum(h.quote_volume_usdt) over (partition by h.coin_key) as share_of_daily_volume
from hourly as h
inner join {{ ref('dim_coin') }} as c using (coin_key)
