-- The main analytics table: one row per coin per day with returns, moving averages,
-- rolling volatility and drawdown. Columns are cast explicitly because this model has an
-- enforced contract (see _marts.yml): if anything upstream changes a type or drops a
-- column, `dbt build` fails here instead of a dashboard silently breaking.
with r as (
    select * from {{ ref('int_returns_daily') }}
),

windows as (
    select
        r.*,
        row_number() over w_all                                          as day_number,
        avg(close_price::double) over w_7                                as sma_7,
        avg(close_price::double) over w_30                               as sma_30,
        stddev_samp(log_return) over w_30 * sqrt(365)                    as vol_30,
        avg(quote_volume_usdt::double) over w_7                          as avg_qv_7,
        max(close_price::double) over w_all                              as running_peak
    from r
    window
        w_all as (partition by coin_key order by price_date rows between unbounded preceding and current row),
        w_7   as (partition by coin_key order by price_date rows between 6 preceding and current row),
        w_30  as (partition by coin_key order by price_date rows between 29 preceding and current row)
)

select
    w.coin_key::bigint                                                   as coin_key,
    w.date_key::integer                                                  as date_key,
    w.exchange_key::bigint                                               as exchange_key,
    c.symbol::varchar                                                    as symbol,
    w.price_date::date                                                   as price_date,
    w.open_price::decimal(28, 8)                                         as open_price,
    w.high_price::decimal(28, 8)                                         as high_price,
    w.low_price::decimal(28, 8)                                          as low_price,
    w.close_price::decimal(28, 8)                                        as close_price,
    w.base_volume::decimal(38, 8)                                        as base_volume,
    w.quote_volume_usdt::decimal(38, 8)                                  as quote_volume_usdt,
    w.number_of_trades::bigint                                           as number_of_trades,
    w.minutes_observed::bigint                                           as minutes_observed,
    w.daily_return::double                                               as daily_return,
    w.log_return::double                                                 as log_return,
    w.taker_buy_ratio::double                                            as taker_buy_ratio,
    -- only publish a moving average once the window is full (no misleading early values)
    case when w.day_number >= 7  then w.sma_7  end::double               as sma_7d,
    case when w.day_number >= 30 then w.sma_30 end::double               as sma_30d,
    case when w.day_number >= 30 then w.vol_30 end::double               as volatility_30d_annualized,
    w.avg_qv_7::double                                                   as avg_quote_volume_7d,
    (w.close_price::double / w.running_peak - 1)::double                 as drawdown_from_peak
from windows as w
inner join {{ ref('dim_coin') }} as c using (coin_key)
