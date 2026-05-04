{{ config(materialized='table') }}

with 
cte_match as (
	select 
	*
	from
	{{ ref('stg_matches') }} sm 
	where 1=1
	and sm.status = 'FullTime'
)
, cte_point as (
	select
	season_id ,
	match_id ,
	home_team_id ,
	home_team ,
	home_score_final ,
	away_score_final ,
	away_team_id ,
	away_team ,
	case
		when home_score_final > away_score_final then 3
		when home_score_final = away_score_final then 1
		else 0
	end home_team_points,
	case
		when home_score_final > away_score_final then 0
		when home_score_final = away_score_final then 1
		else 3
	end away_team_points,
	loaded_at ,
	created_at
	from cte_match 
	order by match_id asc
)
, cte_home_point as (
	select
	season_id ,
	home_team_id ,
	home_team ,
	count(distinct match_id) total_home_games ,
	sum(home_team_points) total_home_points ,
	sum(home_score_final ) total_home_scores ,
	sum(away_score_final ) total_away_scores ,
	max(loaded_at) loaded_at ,
	max(created_at) created_at
	from cte_point 
	group by 1,2,3
)
, cte_away_point as (
	select
	season_id ,
	away_team_id  ,
	away_team ,
	count(distinct match_id) total_away_games ,
	sum(away_team_points) total_away_points ,
	sum(away_score_final ) total_away_scores ,
	sum(home_score_final ) total_home_scores ,
	max(loaded_at) loaded_at ,
	max(created_at) created_at
	from cte_point 
	group by 1,2,3
)
, cte_table as (
	select
	h.season_id ,
	h.home_team_id team_id ,
	h.home_team team_name ,
	h.total_home_games ,
	a.total_away_games ,
	h.total_home_games + a.total_away_games total_games ,
	h.total_home_points ,
	h.total_home_scores ,
	a.total_away_points ,
	a.total_away_scores ,
	h.total_home_scores + a.total_away_scores goal_scored ,
	h.total_away_scores + a.total_home_scores goal_against ,
	(h.total_home_scores + a.total_away_scores) - (h.total_away_scores + a.total_home_scores) goal_difference,
	h.total_home_points + a.total_away_points total_points ,
	h.loaded_at ,
	h.created_at 
	from
	cte_home_point h
	full outer join cte_away_point a
		on a.away_team_id  = h.home_team_id 
		and a.season_id = h.season_id 
)
select
*
from
cte_table
where  1=1
order by season_id, total_points desc, goal_scored desc