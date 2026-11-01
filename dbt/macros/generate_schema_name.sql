{% macro generate_schema_name(custom_schema_name, node) -%}
    {#- Use the folder-level schema as-is so models land in staging,
        intermediate and marts instead of prefixing the target schema. -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
