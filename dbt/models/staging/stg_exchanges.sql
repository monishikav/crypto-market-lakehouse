select exchange_key, exchange_id, exchange_name, exchange_type, website
from {{ source('gold', 'dim_exchange') }}
