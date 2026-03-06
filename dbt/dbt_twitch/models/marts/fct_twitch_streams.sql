SELECT DISTINCT

    stream_id                     AS stream_key,

    user_id,
    user_login,
    user_name,

    game_id,
    game_name,

    stream_type,
    language,

    title,
    thumbnail_url,

    is_mature,
    viewer_count,

    started_at

FROM {{ ref('int_twitch_streams') }}