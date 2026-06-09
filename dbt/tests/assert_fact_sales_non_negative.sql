-- Singular data test — replaces DQ check 2 ("no negative sales").
-- Returns offending rows; the test fails if any are returned.
select
    sale_key,
    order_id,
    sales
from {{ ref('fact_sales') }}
where sales < 0
