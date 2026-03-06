SELECT DISTINCT

    stream_id      AS stream_key,
    tag            AS tag_name

FROM {{ ref('int_twitch_streams') }}