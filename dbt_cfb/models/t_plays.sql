{{
    config(
        materialized='incremental'
    )
}}


with tmp as (
    select
    play_json->>'id' as play_id
    , play_json->>'sequenceNumber' as seq_num
    , play_json->>'scoringPlay' as scoring_play
    , play_json->'period'->>'number' as quarter_id
    , play_json->'clock'->>'displayValue' as start_clock
    , play_json->'start'->'team'->>'id' as poss_team_id
    , play_json->'start'->>'down' as down
    , play_json->'start'->>'distance' as distance
    , play_json->'start'->>'yardLine' as yardline
    , play_json->'type'->>'id' as playtype_id
    , play_json->>'text' as play_detail
    , play_json->>'statYardage' as yards_gained
    , etl_ts
    from
    plays_raw
)

select
play_id::bigint
, seq_num::int
, scoring_play
, quarter_id
, start_clock
, poss_team_id
, down
, distance
, yardLine
, playtype_id::int
, play_detail
, yards_gained
, current_timestamp::timestamp as elt_ts
from
tmp

{% if is_incremental() %}

where etl_ts >= (select max(elt_ts) from {{ this }})

{% endif %}