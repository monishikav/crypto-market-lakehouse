-- Reconciliation: rolling minutes up to days must not create or lose volume.
with minute_total as (
    select sum(quote_volume_usdt) as v from {{ ref('stg_prices_minute') }}
),
daily_total as (
    select sum(quote_volume_usdt) as v from {{ ref('fct_daily_prices') }}
)
select m.v as minute_volume, d.v as daily_volume
from minute_total as m, daily_total as d
where abs(m.v - d.v) > 0.0001
