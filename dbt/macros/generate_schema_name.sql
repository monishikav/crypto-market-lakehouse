{# Use the folder schema as-is (staging, marts, ...) instead of dbt's default
   "<target_schema>_<custom_schema>" prefixing - cleaner names for a single-user warehouse. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
