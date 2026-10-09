select
    date_key,
    full_date,
    year,
    quarter,
    month,
    month_name,
    day_of_month,
    day_of_week,          -- 0 = Monday
    day_name,
    week_of_year,
    is_weekend
from {{ source('gold', 'dim_date') }}
