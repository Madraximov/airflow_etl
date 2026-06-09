{#
    Use the schema configured on each model verbatim (e.g. `staging`, `dwh`,
    `datamart`) instead of dbt's default `<target_schema>_<custom_schema>`
    prefixing. This keeps the warehouse schema names identical to the original
    hand-written DDL so existing queries and BI tools keep working.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
