WITH source AS (
    SELECT * FROM {{ source('silver_staging', 'src_silver_movies') }}
)

SELECT
    letterboxd_uri,
    lb_movie_title,
    lb_movie_year,
    enrichment_status,
    CAST(tmdb_id AS INT64)                                 AS tmdb_id,
    COALESCE(tmdb_title, lb_movie_title)                   AS title,
    tmdb_original_title,
    SAFE_CAST(release_date AS DATE)                        AS release_date,
    EXTRACT(YEAR FROM SAFE_CAST(release_date AS DATE))     AS release_year,
    genres,                                                -- ARRAY<STRING>
    CAST(runtime_minutes AS INT64)                         AS runtime_minutes,
    CAST(budget_usd AS INT64)                              AS budget_usd,
    CAST(revenue_usd AS INT64)                             AS revenue_usd,
    CAST(vote_average AS FLOAT64)                          AS vote_average,
    CAST(vote_count AS INT64)                              AS vote_count,
    overview,
    original_language,
    poster_path,
    director_name,
    CAST(director_tmdb_id AS INT64)                        AS director_tmdb_id,
    CAST(_silver_created_at AS TIMESTAMP)                  AS _silver_created_at,
    CAST(_silver_updated_at AS TIMESTAMP)                  AS _silver_updated_at
FROM source
