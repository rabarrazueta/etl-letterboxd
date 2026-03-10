{{
  config(
    materialized = 'table',
    tags = ['bridge']
  )
}}

WITH movie_genres AS (
    SELECT
        movie_id,
        genre.element AS genre_name -- Entramos al valor real dentro del STRUCT
    FROM {{ ref('dim_movie') }},
    UNNEST(genres.list) AS genre    -- Accedemos a la lista anidada que crea Spark
    WHERE genres IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['mg.movie_id', 'mg.genre_name']) }} AS bridge_id,
    mg.movie_id,
    g.genre_id,
    mg.genre_name,
    {{ dbt.current_timestamp() }} AS _dbt_created_at
FROM movie_genres mg
JOIN {{ ref('dim_genre') }} g ON g.genre_name = mg.genre_name