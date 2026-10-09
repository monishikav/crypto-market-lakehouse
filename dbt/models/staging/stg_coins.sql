select
    coin_key,
    symbol,
    base_asset,
    quote_asset,
    coingecko_id,
    coalesce(coin_name, base_asset)        as coin_name,
    market_cap_rank,
    market_cap_usd,
    circulating_supply,
    max_supply,
    has_supply_cap,
    all_time_high_usd,
    trading_status,
    snapshot_date                          as metadata_snapshot_date
from {{ source('gold', 'dim_coin') }}
