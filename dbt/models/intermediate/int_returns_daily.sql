-- Day-over-day returns. Simple return is what investors quote; log return is what you
-- sum / take std-dev of (log returns add up across days, simple returns don't).
-- Gap-aware: if the previous row isn't literally yesterday (missing data, a listing pause),
-- the return is NULL rather than a fake "one-day" move spanning weeks.
with base as (
    select
        *,
        lag(close_price::double) over w   as prev_close,
        lag(price_date) over w            as prev_date
    from {{ ref('int_prices_daily') }}
    window w as (partition by coin_key order by price_date)
)

select
    * exclude (prev_close, prev_date),
    case when prev_date = price_date - 1
         then close_price::double / prev_close - 1 end                as daily_return,
    case when prev_date = price_date - 1
         then ln(close_price::double / prev_close) end                as log_return,
    taker_buy_base_volume::double / nullif(base_volume::double, 0)   as taker_buy_ratio
from base
