{{
    config(
        materialized = 'table',
        tags         = ['dimension']
    )
}}

WITH all_genres AS (
    SELECT DISTINCT 
        genre.element AS genre_name -- Entramos a la estructura de Spark
    FROM {{ ref('stg_silver_movies') }},
    UNNEST(genres.list) AS genre    -- Spark guarda los arrays como 'list' en BQ
    WHERE genres IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['genre_name']) }} AS genre_id,
    genre_name,
    {{ dbt.current_timestamp() }} AS _dbt_created_at,
    {{ dbt.current_timestamp() }} AS _dbt_updated_at
FROM all_genres
WHERE genre_name IS NOT NULL
ORDER BY genre_name
