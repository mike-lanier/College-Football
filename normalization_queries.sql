-------------------------------------------------------------------------------------------------
                                        -- SCHEDULE TABLE
-------------------------------------------------------------------------------------------------

with tmp as (
select
sched_json->>'id' as game_id
, sched_json->>'date' as game_date
, sched_json->>'name' as matchup_full
, sched_json->>'shortName' as matchup_abbrv
, jsonb_array_elements(jsonb_array_elements(sched_json->'competitions')->'competitors')->>'id' as team_id
, jsonb_array_elements(jsonb_array_elements(sched_json->'competitions')->'competitors')->>'homeAway' as home_away
, jsonb_array_elements(jsonb_array_elements(sched_json->'competitions')->'competitors')->>'winner' as winner
, jsonb_array_elements(sched_json->'competitions')->'venue'->>'id' as venue_id
, jsonb_array_elements(sched_json->'competitions')->'attendance' as attendance
from
schedule_raw
),

conv as (
select
game_id
, game_date::timestamp as game_ts
, matchup_full
, matchup_abbrv
, max(case when home_away = 'home' then team_id::int else null end) as home_team_id
, max(case when winner = 'true' then team_id::int else null end) as winning_team_id
, venue_id::int
, attendance::int
from
tmp
group by
game_id
, game_date::timestamp
, matchup_full
, matchup_abbrv
, venue_id::int
, attendance::int
)

select
game_id
, game_ts::date as game_date
, game_ts::time as game_time
, matchup_full
, matchup_abbrv
, home_team_id
, winning_team_id
, venue_id
, attendance
from
conv;



-------------------------------------------------------------------------------------------------
                                        -- VENUE TABLE
-------------------------------------------------------------------------------------------------


with tmp as (
select
jsonb_array_elements(sched_json->'competitions')->'venue'->>'id' as venue_id
, jsonb_array_elements(sched_json->'competitions')->'venue'->>'fullName' as venue_name
, jsonb_array_elements(sched_json->'competitions')->'venue'->'address'->>'city' as venue_city
, jsonb_array_elements(sched_json->'competitions')->'venue'->'address'->>'state' as venue_state
from
schedule_raw
)

select distinct
venue_id::int
, venue_name
, venue_city
, venue_state
from
tmp;


-------------------------------------------------------------------------------------------------
                                    -- BROADCAST INFO TABLE
-------------------------------------------------------------------------------------------------


with tmp as (
select
sched_json->>'id' as game_id
, jsonb_array_elements(jsonb_array_elements(sched_json->'competitions')->'geoBroadcasts')->'type'->>'shortName' as broadcast_type
, jsonb_array_elements(jsonb_array_elements(sched_json->'competitions')->'geoBroadcasts')->'market'->>'type' as broadcast_market
, jsonb_array_elements(jsonb_array_elements(sched_json->'competitions')->'geoBroadcasts')->'media'->>'shortName' as broadcast_network
from
schedule_raw
)

select distinct
game_id::int
, broadcast_type
, broadcast_market
, broadcast_network
from
tmp;