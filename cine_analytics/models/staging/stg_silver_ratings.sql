WITH source AS (
    SELECT * FROM {{ source('silver_staging', 'src_silver_ratings') }}
)

SELECT
    letterboxd_uri,
    movie_title,
    SAFE_CAST(rating_date AS DATE)        AS rating_date,
    CAST(my_rating AS FLOAT64)            AS my_rating,
    CAST(_silver_created_at AS TIMESTAMP) AS _silver_created_at,
    CAST(_silver_updated_at AS TIMESTAMP) AS _silver_updated_at
FROM source
WHERE letterboxd_uri IS NOT NULL
  AND rating_date    IS NOT NULL
  AND my_rating      IS NOT NULL
