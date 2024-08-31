{{
    config(
        materialized='incremental'
    )
}}


with tmp as (
    select
    score_json->>'id' as play_id
    , score_json->'type'->>'id' as playtype_id
    , score_json->>'text' as play_detail
    , score_json->'team'->>'id' as team_id
    , score_json->'period'->>'number' as quarter_id
    , score_json->'clock'->>'displayValue' as game_clock
    , split_part(filename, '.json', 1) as game_id
    , etl_ts
    from
    landing.raw_scoring_plays
)

select
play_id::bigint
, game_id::int
, quarter_id
, game_clock
, team_id::int
, playtype_id::int
, play_detail
, current_timestamp::timestamp as elt_ts
from
tmp

{% if is_incremental() %}

where game_id::int not in (select distinct game_id from {{ this }})

{% endif %}