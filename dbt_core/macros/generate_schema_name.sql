{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Override behavior mặc định của dbt:
        - Mặc định: dataset_gốc + "_" + custom_schema  →  analytics_485408210_stg  ❌
        - Sau override: chỉ dùng đúng custom_schema    →  stg                      ✅
    #}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
