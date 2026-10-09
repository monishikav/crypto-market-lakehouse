-- Roll 1,440 minute candles up to 1 daily candle per coin.
-- open  = the FIRST minute's open, close = the LAST minute's close (arg_min / arg_max by time),
-- not min/max of the prices - a classic OHLC aggregation mistake.
select
    coin_key,
    date_key,
    exchange_key,
    cast(open_time_utc as date)                         as price_date,
    arg_min(open_price, open_time_utc)                  as open_price,
    max(high_price)                                     as high_price,
    min(low_price)                                      as low_price,
    arg_max(close_price, open_time_utc)                 as close_price,
    sum(base_volume)                                    as base_volume,
    sum(quote_volume_usdt)                              as quote_volume_usdt,
    cast(sum(number_of_trades) as bigint)               as number_of_trades,
    sum(taker_buy_base_volume)                          as taker_buy_base_volume,
    count(*)                                            as minutes_observed
from {{ ref('stg_prices_minute') }}
group by all
