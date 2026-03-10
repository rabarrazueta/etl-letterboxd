{{
    config(
        materialized = 'table',
        tags         = ['dimension', 'static']
    )
}}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "cast('" ~ var('date_spine_start') ~ "' as date)",
        end_date   = "cast('" ~ var('date_spine_end')   ~ "' as date)"
    ) }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_day']) }}  AS date_id,
    date_day                                               AS full_date,
    EXTRACT(YEAR    FROM date_day)                         AS year,
    EXTRACT(QUARTER FROM date_day)                         AS quarter_number,
    {{ format_year_quarter('date_day') }}                  AS year_quarter,
    EXTRACT(MONTH   FROM date_day)                         AS month_number,
    {{ format_month_name('date_day') }}                    AS month_name,
    EXTRACT(DAY     FROM date_day)                         AS day_of_month,
    {{ day_of_week('date_day') }}                          AS day_of_week_number,
    {{ format_day_name('date_day') }}                      AS day_of_week_name,
    CASE WHEN {{ day_of_week('date_day') }} IN (1, 7)
         THEN TRUE ELSE FALSE END                          AS is_weekend,
    EXTRACT(WEEK    FROM date_day)                         AS week_of_year,
    {{ dbt.current_timestamp() }}                          AS _dbt_created_at,
    {{ dbt.current_timestamp() }}                          AS _dbt_updated_at
FROM date_spine
