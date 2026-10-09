-- One row per coin: the "scorecard" a portfolio dashboard would show.
with daily as (
    select * from {{ ref('fct_daily_prices') }}
),

ranked_days as (
    select
        coin_key,
        arg_max(price_date, daily_return)  as best_day,
        max(daily_return)                  as best_day_return,
        arg_min(price_date, daily_return)  as worst_day,
        min(daily_return)                  as worst_day_return
    from daily
    where daily_return is not null
    group by coin_key
),

summary as (
    select
        coin_key,
        min(price_date)                                         as first_date,
        max(price_date)                                         as last_date,
        arg_min(open_price, price_date)::double                 as start_price,
        arg_max(close_price, price_date)::double                as end_price,
        stddev_samp(log_return) * sqrt(365)                     as annualized_volatility,
        avg(daily_return) * 365                                 as annualized_mean_return,
        min(drawdown_from_peak)                                 as max_drawdown,
        avg(quote_volume_usdt::double)                          as avg_daily_quote_volume_usdt,
        avg(case when daily_return > 0 then 1.0 else 0.0 end)   as pct_up_days
    from daily
    group by coin_key
)

select
    c.coin_key::bigint                                                   as coin_key,
    c.symbol::varchar                                                    as symbol,
    c.coin_name::varchar                                                 as coin_name,
    c.market_cap_rank::bigint                                            as market_cap_rank,
    s.first_date::date                                                   as first_date,
    s.last_date::date                                                    as last_date,
    s.start_price::double                                                as start_price,
    s.end_price::double                                                  as end_price,
    (s.end_price / s.start_price - 1)::double                            as period_return,
    s.annualized_volatility::double                                      as annualized_volatility,
    -- risk-adjusted return (risk-free rate assumed 0 for simplicity)
    (s.annualized_mean_return / nullif(s.annualized_volatility, 0))::double as sharpe_ratio,
    s.max_drawdown::double                                               as max_drawdown,
    r.best_day::date                                                     as best_day,
    r.best_day_return::double                                            as best_day_return,
    r.worst_day::date                                                    as worst_day,
    r.worst_day_return::double                                           as worst_day_return,
    s.pct_up_days::double                                                as pct_up_days,
    s.avg_daily_quote_volume_usdt::double                                as avg_daily_quote_volume_usdt,
    (rank() over (order by s.end_price / s.start_price desc))::bigint    as return_rank
from summary as s
inner join {{ ref('dim_coin') }} as c using (coin_key)
left join ranked_days as r using (coin_key)
