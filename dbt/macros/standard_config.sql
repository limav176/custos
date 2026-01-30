{% macro standard_config(partitioned_by_column=none, materialized=none, incremental_strategy='append', on_schema_change='append_new_columns') %}
    {% set cfg = {
        'properties': {'format': "'PARQUET'"},
        'on_schema_change': on_schema_change,
        'incremental_strategy': incremental_strategy
    } %}

    {% if partitioned_by_column is not none %}
        {% do cfg['properties'].update({'partitioned_by': partitioned_by_column}) %}
        {% do cfg.update({'pre_hook': "set session hive.insert_existing_partitions_behavior='OVERWRITE'"}) %}
    {% endif %}

    {% if materialized is not none %}
        {% do cfg.update({'materialized': materialized}) %}
    {% endif %}

    {{ config(**cfg) }}
{% endmacro %}
