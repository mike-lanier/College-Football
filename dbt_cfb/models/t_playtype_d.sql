with tmp as (
    select
    play_json->'type'->>'id' as playtype_id
    , play_json->'type'->>'abbreviation' as playtype_abbrv
    , play_json->'type'->>'text' as playtype_detail
    from
    landing.raw_plays

    union

    select
    play_json->'pointAfterAttempt'->>'id' as playtype_id
    , play_json->'pointAfterAttempt'->>'abbreviation' as playtype_abbrv
    , play_json->'pointAfterAttempt'->>'text' as playtype_detail
    from
    landing.raw_plays
)

select distinct
playtype_id::int
, playtype_abbrv
, playtype_detail
from
tmp
where
playtype_id is not null