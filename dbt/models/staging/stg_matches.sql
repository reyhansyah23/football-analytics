with source as (
    select * from {{ source('raw_data', 'raw_matches') }}
)
, dedup as (
    select
        *,
        row_number() over(partition by match_id, season_id order by loaded_at desc) as rn
    from source
)
, renamed as (
    select
        md5(match_id::varchar||season_id::varchar) as match_season_id,
        match_id::varchar as match_id,
        season_id::varchar as season_id,
        matchweek::int as matchweek,
        kickoff::timestamp as kickoff_at,
  
        status::varchar as status,
        result_type::varchar as result_type,
        ground::varchar as ground,  

        home_team::varchar as home_team,
        home_team_id::varchar as home_team_id,
        home_score_ht::int as home_score_halftime,
        home_score::int as home_score_final,
        home_redcard::int as home_redcards,
        
        away_team::varchar as away_team,
        away_team_id::varchar as away_team_id,
        away_score_ht::int as away_score_halftime,
        away_score::int as away_score_final,
        away_redcard::int as away_redcards,
        
        attendance::int as attendance,
        loaded_at::timestamp as loaded_at,

        now() as created_at
    from dedup
    where rn = 1
)
select * from renamed