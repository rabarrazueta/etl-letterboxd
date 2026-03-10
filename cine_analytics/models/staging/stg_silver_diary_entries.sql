WITH source AS (
    SELECT * FROM {{ source('silver_staging', 'src_silver_diary_entries') }}
)

SELECT
    letterboxd_uri,
    movie_title,
    SAFE_CAST(watched_date AS DATE)       AS watched_date,
    SAFE_CAST(diary_date   AS DATE)       AS diary_date,
    CAST(my_rating AS FLOAT64)            AS my_rating,
    CAST(is_rewatch AS BOOL)              AS is_rewatch,
    tags,                                 -- ARRAY<STRING>
    CAST(_silver_created_at AS TIMESTAMP) AS _silver_created_at,
    CAST(_silver_updated_at AS TIMESTAMP) AS _silver_updated_at
FROM source
WHERE letterboxd_uri IS NOT NULL
  AND watched_date   IS NOT NULL
