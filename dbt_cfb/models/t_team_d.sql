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