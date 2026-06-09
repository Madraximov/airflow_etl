-- Date dimension covering the full span of order/ship dates seen in the data.
-- date_key is an integer in YYYYMMDD form, matching the original star schema.
with bounds as (
    select
        min(order_date) as min_date,
        max(ship_date)  as max_date
    from {{ ref('stg_orders') }}
),

spine as (
    select generate_series(
        (select min_date from bounds),
        (select max_date from bounds),
        interval '1 day'
    )::date as full_date
)

select
    cast(to_char(full_date, 'YYYYMMDD') as integer) as date_key,
    full_date,
    extract(year    from full_date)::integer        as year,
    extract(quarter from full_date)::integer        as quarter,
    extract(month   from full_date)::integer        as month,
    to_char(full_date, 'Month')                     as month_name,
    extract(week    from full_date)::integer        as week,
    extract(isodow  from full_date)::integer        as day_of_week,
    to_char(full_date, 'Day')                       as day_name,
    extract(isodow  from full_date) >= 6            as is_weekend
from spine
