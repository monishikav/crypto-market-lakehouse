-- 1 row per coin per minute. Renames + casts only; no business logic in staging.
select
    coin_key,
    date_key,
    exchange_key,
    -- Delta stores UTC instants; the profile pins TimeZone=UTC so this cast is exact.
    cast(open_time as timestamp)           as open_time_utc,
    open                                   as open_price,
    high                                   as high_price,
    low                                    as low_price,
    close                                  as close_price,
    base_volume,
    quote_volume_usdt,
    number_of_trades,
    taker_buy_base_volume
from {{ source('gold', 'fact_prices') }}
