-- Geography dimension: one row per distinct city/state/country, carrying the
-- region. Surrogate key hashes the natural grain used by the fact join.
with geography as (
    select distinct
        city,
        state,
        country,
        region
    from {{ ref('stg_orders') }}
)

select
    md5(city || '|' || state || '|' || country) as geo_key,
    city,
    state,
    country,
    region
from geography
