-- Sanity check on the correlation logic: BTC vs BTC must be exactly 1.
select *
from {{ ref('mart_btc_correlation') }}
where symbol = 'BTCUSDT' and abs(correlation_with_btc - 1) > 1e-9
