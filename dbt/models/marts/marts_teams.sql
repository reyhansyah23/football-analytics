{{ config(materialized='table') }}

select
    team_id,
    team_name,
    short_name
from {{ ref('stg_teams') }}