-- Is there a day-of-week effect? (Crypto trades 24/7, so weekends are real trading days.)
select
    f.symbol,
    d.day_of_week,
    d.day_name,
    d.is_weekend,
    count(*)                                                    as days,
    avg(f.daily_return)                                         as avg_daily_return,
    avg(case when f.daily_return > 0 then 1.0 else 0.0 end)     as pct_up_days,
    avg(f.quote_volume_usdt::double)                            as avg_quote_volume_usdt
from {{ ref('fct_daily_prices') }} as f
inner join {{ ref('dim_date') }} as d using (date_key)
where f.daily_return is not null
group by all
