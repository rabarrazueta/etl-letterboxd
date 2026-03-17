{% snapshot snap_dim_movie %}

{{
    config(
        unique_key    = 'letterboxd_uri',
        strategy      = 'check',
        check_cols    = [
            'vote_average',
            'vote_count',
            'runtime_minutes',
            'overview',
            'director_name',
            'budget_usd',
            'revenue_usd'
        ],
        invalidate_hard_deletes = False
    )
}}

SELECT
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
    director_name,
    director_tmdb_id,
    _silver_updated_at
FROM {{ ref('stg_silver_movies') }}
WHERE enrichment_status IN ('SUCCESS', 'FUZZY_MATCH')

{% endsnapshot %}
