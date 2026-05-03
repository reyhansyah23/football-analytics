with source as (
    select * from {{ source('raw_data', 'raw_teams') }}
)
, dedup as (
    select
        *,
        row_number() over(partition by team_id, season_id order by loaded_at desc) as rn
    from source
)
, renamed as (
    select
        md5(team_id::varchar||season_id::varchar) as team_season_id,
        team_id::varchar as team_id,
        season_id::varchar as season_id,
        team_name::varchar as team_name,
        short_name::varchar as short_name,
        abbreviation::varchar as abbreviation,
        stadium_name::varchar as stadium_name,
        city::varchar as city,
        capacity::int as capacity,
        country::varchar as country,
        loaded_at::timestamp as loaded_at,
        now() as created_at
    from dedup
    where rn = 1
)
select * from renamed