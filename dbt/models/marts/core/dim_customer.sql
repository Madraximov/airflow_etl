-- Customer dimension. Surrogate key is a deterministic hash of the natural
-- key so the fact table can be rebuilt independently and idempotently.
-- SCD-Type-2 scaffolding (valid_from / valid_to / is_current) is kept so the
-- model can later be converted to a snapshot.
with customers as (
    select * from {{ ref('stg_customers') }}
)

select
    md5(customer_id)            as customer_key,
    customer_id,
    customer_name,
    segment,
    now()                       as valid_from,
    cast(null as timestamp)     as valid_to,
    true                        as is_current
from customers
