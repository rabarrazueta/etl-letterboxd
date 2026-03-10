-- Todas las macros de portabilidad BigQuery ↔ PostgreSQL en un solo archivo.
-- Agregar aquí cualquier función nueva con diferencia de sintaxis entre targets.

{% macro format_month_name(date_col) %}
  {%- if target.type == 'bigquery' -%}
    FORMAT_DATE('%B', {{ date_col }})
  {%- elif target.type == 'postgres' -%}
    TRIM(TO_CHAR({{ date_col }}, 'Month'))
  {%- else -%}
    {{ exceptions.raise_compiler_error("Target no soportado: " ~ target.type) }}
  {%- endif -%}
{% endmacro %}

{% macro format_day_name(date_col) %}
  {%- if target.type == 'bigquery' -%}
    FORMAT_DATE('%A', {{ date_col }})
  {%- elif target.type == 'postgres' -%}
    TRIM(TO_CHAR({{ date_col }}, 'Day'))
  {%- else -%}
    {{ exceptions.raise_compiler_error("Target no soportado: " ~ target.type) }}
  {%- endif -%}
{% endmacro %}

{% macro day_of_week(date_col) %}
  {%- if target.type == 'bigquery' -%}
    EXTRACT(DAYOFWEEK FROM {{ date_col }})
  {%- elif target.type == 'postgres' -%}
    CAST(EXTRACT(DOW FROM {{ date_col }}) + 1 AS INTEGER)
  {%- else -%}
    {{ exceptions.raise_compiler_error("Target no soportado: " ~ target.type) }}
  {%- endif -%}
{% endmacro %}

{% macro format_year_quarter(date_col) %}
  {%- if target.type == 'bigquery' -%}
    CONCAT(
      CAST(EXTRACT(YEAR    FROM {{ date_col }}) AS STRING), '-Q',
      CAST(EXTRACT(QUARTER FROM {{ date_col }}) AS STRING)
    )
  {%- elif target.type == 'postgres' -%}
    CONCAT(
      CAST(EXTRACT(YEAR    FROM {{ date_col }}) AS TEXT), '-Q',
      CAST(EXTRACT(QUARTER FROM {{ date_col }}) AS TEXT)
    )
  {%- else -%}
    {{ exceptions.raise_compiler_error("Target no soportado: " ~ target.type) }}
  {%- endif -%}
{% endmacro %}

{% macro epoch_timestamp() %}
  {%- if target.type == 'bigquery' -%}
    TIMESTAMP('1900-01-01 00:00:00 UTC')
  {%- elif target.type == 'postgres' -%}
    CAST('1900-01-01 00:00:00' AS TIMESTAMP)
  {%- else -%}
    {{ exceptions.raise_compiler_error("Target no soportado: " ~ target.type) }}
  {%- endif -%}
{% endmacro %}

{% macro safe_divide(numerator, denominator) %}
  CASE
    WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL THEN NULL
    ELSE {{ numerator }} / {{ denominator }}
  END
{% endmacro %}
