{{
    config(
        materialized = 'table',
        tags         = ['dimension']
    )
}}

WITH directors AS (
    SELECT DISTINCT
        director_tmdb_id,
        director_name
    FROM {{ ref('stg_silver_movies') }}
    WHERE director_name     IS NOT NULL
      AND director_tmdb_id  IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['director_tmdb_id']) }} AS director_id,
    director_tmdb_id,
    director_name,
    {{ dbt.current_timestamp() }} AS _dbt_created_at,
    {{ dbt.current_timestamp() }} AS _dbt_updated_at
FROM directors
