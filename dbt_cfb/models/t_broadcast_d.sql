{{
    config(
        materialized='incremental'
    )
}}

with tmp as (
select
sched_json->>'id' as game_id
, jsonb_array_elements(jsonb_array_elements(sched_json->'competitions')->'geoBroadcasts')->'type'->>'shortName' as broadcast_type
, jsonb_array_elements(jsonb_array_elements(sched_json->'competitions')->'geoBroadcasts')->'market'->>'type' as broadcast_market
, jsonb_array_elements(jsonb_array_elements(sched_json->'competitions')->'geoBroadcasts')->'media'->>'shortName' as broadcast_network
from
landing.raw_schedule
)

select distinct
game_id::int
, broadcast_type
, broadcast_market
, broadcast_network
from
tmp

{% if is_incremental() %}

where game_id::int not in (select game_id from {{ this }})

{% endif %}