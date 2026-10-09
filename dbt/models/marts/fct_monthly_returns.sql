select
    d.coin_key,
    d.symbol,
    date_trunc('month', d.price_date)::date                        as month_start,
    arg_min(d.open_price, d.price_date)                            as month_open,
    arg_max(d.close_price, d.price_date)                           as month_close,
    max(d.high_price)                                              as month_high,
    min(d.low_price)                                               as month_low,
    arg_max(d.close_price, d.price_date)::double
        / arg_min(d.open_price, d.price_date)::double - 1          as monthly_return,
    sum(d.quote_volume_usdt)                                       as quote_volume_usdt,
    count(*)                                                       as trading_days
from {{ ref('fct_daily_prices') }} as d
group by all
