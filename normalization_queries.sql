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


-------------------------------------------------------------------------------------------------
                                    -- TEAMS TABLE
-------------------------------------------------------------------------------------------------


with tmp as (
    select
    team_json->'team'->>'id' as team_id
    , team_json->'team'->>'location' as school
    , team_json->'team'->>'name' as mascot
    , team_json->'team'->>'displayName' as team_desc_full
    , team_json->'team'->>'shortDisplayName' as team_desc_short
    from
    teams_raw
)

select distinct
team_id::int
, school
, mascot
, team_desc_full
, team_desc_short
from
tmp


-------------------------------------------------------------------------------------------------
                                    -- PLAYS TABLE
-------------------------------------------------------------------------------------------------


with tmp as (
    select
    play_json->>'id' as play_id
    , play_json->>'sequenceNumber' as seq_num
    , play_json->>'scoringPlay' as scoring_play
    , play_json->'period'->>'number' as quarter_id
    , play_json->'clock'->>'displayValue' as start_clock
    , play_json->'start'->'team'->>'id' as poss_team_id
    , play_json->'start'->>'down' as start_down
    , play_json->'start'->>'distance' as start_distance
    , play_json->'start'->>'yardLine' as start_yardline
    , play_json->'start'->>'downDistanceText' as start_poss_detail
    , play_json->'type'->>'id' as playtype_id
    , play_json->>'text' as play_detail
    , play_json->>'statYardage' as yards_gained
    , play_json->'end'->'team'->>'id' as end_poss_team_id
    , play_json->'end'->>'down' as end_down
    , play_json->'end'->>'distance' as end_distance
    , play_json->'end'->>'yardLine' as end_yardline
    , play_json->'end'->>'downDistanceText' as end_poss_detail
    from
    plays_raw
)


select
play_json->'start'->>'down' as start_down
, play_json->'start'->>'distance' as start_distance
, play_json->'start'->>'yardLine' as start_yardline
, play_json->'type'->>'id' as playtype_id
, play_json->>'statYardage' as yards_gained
, play_json->'end'->'team'->>'id' as end_poss_team_id
, play_json->'end'->>'down' as end_down
, play_json->'end'->>'distance' as end_distance
, play_json->'end'->>'yardLine' as end_yardline
from
plays_raw


select
play_json
from
plays_raw
limit 1;


-------------------------------------------------------------------------------------------------
                                    -- PLAYTYPE DETAIL TABLE
-------------------------------------------------------------------------------------------------



with tmp as (
    select
    play_json->'type'->>'id' as playtype_id
    , play_json->'type'->>'abbreviation' as playtype_abbrv
    , play_json->'type'->>'text' as playtype_detail
    from
    plays_raw

    union

    select
    play_json->'pointAfterAttempt'->>'id' as playtype_id
    , play_json->'pointAfterAttempt'->>'abbreviation' as playtype_abbrv
    , play_json->'pointAfterAttempt'->>'text' as playtype_detail
    from
    plays_raw
)

select distinct
playtype_id::int
, playtype_abbrv
, playtype_detail
from
tmp




-- truncate table t_schedule, t_venue_d, t_broadcast_d, t_playtype_d, t_plays;
drop table t_schedule, t_venue_d, t_broadcast_d, t_playtype_d, t_plays, t_team_d;