-- Cleaned, de-duplicated customer records from the raw landing table.
with source as (
    select * from {{ source('staging', 'customers') }}
),

cleaned as (
    select distinct
        trim(customer_id)            as customer_id,
        trim(customer_name)          as customer_name,
        trim(segment)                as segment
    from source
    where customer_id is not null
)

select * from cleaned
