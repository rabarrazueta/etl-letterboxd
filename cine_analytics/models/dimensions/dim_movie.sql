{{
    config(
        materialized = 'table',
        tags         = ['dimension', 'scd2']
    )
}}

-- Materializa SOLO la versión actual del SCD2 (dbt_valid_to IS NULL).
-- Para consultar el historial completo de cambios: usar snap_dim_movie directamente.

WITH current_snapshot AS (
    SELECT *
    FROM {{ ref('snap_dim_movie') }}
    WHERE dbt_valid_to IS NULL
),

enriched AS (
    SELECT
        cs.*,
        d.director_id
    FROM current_snapshot cs
    LEFT JOIN {{ ref('dim_director') }} d
           ON cs.director_tmdb_id = d.director_tmdb_id
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['letterboxd_uri']) }} AS movie_id,
    dbt_scd_id                     AS movie_version_id,
    letterboxd_uri,
    tmdb_id,
    title,
    tmdb_original_title,
    release_date,
    release_year,
    genres,
    runtime_minutes,
    budget_usd,
    revenue_usd,
    vote_average,
    vote_count,
    overview,
    original_language,
    poster_path,
    director_id,
    director_name,
    dbt_valid_from                 AS version_valid_from,
    TRUE                           AS is_current_version,
    {{ dbt.current_timestamp() }}  AS _dbt_created_at,
    {{ dbt.current_timestamp() }}  AS _dbt_updated_at
FROM enriched
