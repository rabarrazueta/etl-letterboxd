{{
    config(
        materialized  = 'incremental',
        unique_key    = 'diary_entry_id',
        on_schema_change = 'sync_all_columns',
        tags          = ['fact']
    )
}}

WITH diary AS (
    SELECT * FROM {{ ref('stg_silver_diary_entries') }}

    {% if is_incremental() %}
    -- Solo procesar registros más nuevos que el último _dbt_updated_at en la tabla
    WHERE _silver_updated_at > (
        SELECT COALESCE(MAX(_dbt_updated_at), {{ epoch_timestamp() }})
        FROM {{ this }}
    )
    {% endif %}
),

enriched AS (
    SELECT
        d.letterboxd_uri,
        d.watched_date,
        d.diary_date,
        d.my_rating,
        d.is_rewatch,
        d.tags,
        m.movie_id,
        dt.date_id    AS watched_date_id,
        d._silver_updated_at
    FROM diary d
    LEFT JOIN {{ ref('dim_movie') }} m
           ON d.letterboxd_uri = m.letterboxd_uri
    LEFT JOIN {{ ref('dim_date') }} dt
           ON d.watched_date = dt.full_date
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['letterboxd_uri', 'watched_date']) }} AS diary_entry_id,
    movie_id,
    watched_date_id,
    letterboxd_uri,
    watched_date,
    diary_date,
    my_rating,
    is_rewatch,
    tags,
    {{ dbt.current_timestamp() }} AS _dbt_created_at,
    {{ dbt.current_timestamp() }} AS _dbt_updated_at
FROM enriched
