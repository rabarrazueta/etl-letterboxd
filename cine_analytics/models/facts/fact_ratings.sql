{{
    config(
        materialized  = 'incremental',
        unique_key    = 'rating_id',
        on_schema_change = 'sync_all_columns',
        tags          = ['fact']
    )
}}

WITH ratings AS (
    SELECT * FROM {{ ref('stg_silver_ratings') }}

    {% if is_incremental() %}
    WHERE _silver_updated_at > (
        SELECT COALESCE(MAX(_dbt_updated_at), {{ epoch_timestamp() }})
        FROM {{ this }}
    )
    {% endif %}
),

enriched AS (
    SELECT
        r.letterboxd_uri,
        r.rating_date,
        r.my_rating,
        m.movie_id,
        dt.date_id AS rating_date_id,
        r._silver_updated_at
    FROM ratings r
    LEFT JOIN {{ ref('dim_movie') }} m
           ON r.letterboxd_uri = m.letterboxd_uri
    LEFT JOIN {{ ref('dim_date') }} dt
           ON r.rating_date = dt.full_date
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['letterboxd_uri', 'rating_date']) }} AS rating_id,
    movie_id,
    rating_date_id,
    letterboxd_uri,
    rating_date,
    my_rating,
    {{ dbt.current_timestamp() }} AS _dbt_created_at,
    {{ dbt.current_timestamp() }} AS _dbt_updated_at
FROM enriched
