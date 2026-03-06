WITH
flatten_raw_json AS (
    SELECT
        stream.value:id::VARCHAR              AS stream_id,
        stream.value:user_id::VARCHAR         AS user_id,
        stream.value:user_login::VARCHAR      AS user_login,
        stream.value:user_name::VARCHAR       AS user_name,

        stream.value:game_id::VARCHAR         AS game_id,
        stream.value:game_name::VARCHAR       AS game_name,

        stream.value:type::VARCHAR            AS stream_type,
        stream.value:language::VARCHAR        AS language,

        stream.value:title::VARCHAR           AS title,
        stream.value:thumbnail_url::VARCHAR   AS thumbnail_url,

        stream.value:is_mature::BOOLEAN       AS is_mature,
        stream.value:viewer_count::INT        AS viewer_count,

        stream.value:started_at::TIMESTAMP    AS started_at,

        stream.value:tags                     AS tags_array

    FROM {{ ref('stg_twitch_streams') }},
    LATERAL FLATTEN(input => raw_json:data) stream
),
flatten_tags AS (
    SELECT
        stream_id,
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
        started_at,

        tag.value::VARCHAR AS tag

    FROM flatten_raw_json,
    LATERAL FLATTEN(input => tags_array) tag
)
SELECT * FROM flatten_tags