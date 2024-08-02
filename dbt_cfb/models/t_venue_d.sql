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
tmp