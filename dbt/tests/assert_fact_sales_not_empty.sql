-- Singular data test — replaces DQ check 3 ("fact_sales is empty!").
-- Returns a row only when the fact table came out empty, failing the run.
select 1 as is_empty
from (select count(*) as n from {{ ref('fact_sales') }}) c
where c.n = 0
