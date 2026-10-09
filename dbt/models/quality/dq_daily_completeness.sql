-- Data-quality model: every coin should have 1,440 one-minute candles per UTC day.
-- Gaps mean an exchange outage, a delisting, or a broken ingestion - worth knowing about.
select
    f.symbol,
    f.price_date,
    f.minutes_observed,
    1440 - f.minutes_observed                       as missing_minutes
from {{ ref('fct_daily_prices') }} as f
