-- How much does each coin move with Bitcoin? correlation in [-1, 1]; beta = sensitivity
-- (beta 1.5 => on average moves 1.5% when BTC moves 1%).
with returns as (
    select coin_key, symbol, price_date, log_return
    from {{ ref('fct_daily_prices') }}
    where log_return is not null
),

btc as (
    select price_date, log_return as btc_log_return
    from returns
    where symbol = 'BTCUSDT'
)

select
    r.coin_key,
    r.symbol,
    count(*)                                                     as overlapping_days,
    corr(r.log_return, b.btc_log_return)                         as correlation_with_btc,
    covar_samp(r.log_return, b.btc_log_return)
        / nullif(var_samp(b.btc_log_return), 0)                  as beta_to_btc
from returns as r
inner join btc as b using (price_date)
group by all
