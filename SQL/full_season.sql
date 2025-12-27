WITH total_matchday_cte AS (
    SELECT 
        l.league_id,
        l.name AS league_name,
        COUNT(DISTINCT t.team_id) AS total_teams,
        MAX(m.matchday) AS total_matchday
    FROM matches m
    INNER JOIN leagues l ON m.league_id = l.league_id
    INNER JOIN teams t ON t.league_id = l.league_id
    GROUP BY l.league_id, l.name
),
matchday_cte AS (
    SELECT 
        l.league_id,
        l.name AS league_name,
        l.country,
        t.team_id,
        t.name AS club_name,
        m.matchday,
        m.`utc_date` AS matchday_date,
        t2.name AS home_club,
        s.full_time_home,
        s.full_time_away,
        t3.name AS away_club,
        s2.name AS stadium_played,
        m.winner,
        CASE
            WHEN (t.team_id = m.home_team_id AND m.winner = 'HOME_TEAM') OR (t.team_id = m.away_team_id AND m.winner = 'AWAY_TEAM') THEN 3
            WHEN (t.team_id = m.home_team_id AND m.winner = 'AWAY_TEAM') OR (t.team_id = m.away_team_id AND m.winner = 'HOME_TEAM') THEN 0
            ELSE 1
        END AS points,
        CASE
            WHEN t.team_id = m.home_team_id THEN s.full_time_home
            WHEN t.team_id = m.away_team_id THEN s.full_time_away
        END AS goal_scored,
        CASE
            WHEN t.team_id = m.home_team_id THEN s.full_time_away
            WHEN t.team_id = m.away_team_id THEN s.full_time_home
        END AS goal_against
    FROM teams t
    LEFT JOIN leagues l ON l.league_id = t.league_id 
    LEFT JOIN matches m ON m.home_team_id = t.team_id OR m.away_team_id = t.team_id
    LEFT JOIN teams t2 ON t2.team_id = m.home_team_id
    LEFT JOIN teams t3 ON t3.team_id = m.away_team_id
    LEFT JOIN stadiums s2 ON s2.stadium_id = t2.stadium_id
    LEFT JOIN scores s ON s.match_id = m.match_id 
),
point_cte AS (
    SELECT
        matchday_cte.*,
        IF(points = 3, 1, 0) AS win,
        IF(points = 1, 1, 0) AS draw,
        IF(points = 0, 1, 0) AS lose,
        SUM(IF(points = 3, 1, 0)) OVER(PARTITION BY team_id ORDER BY matchday ASC) AS win_by_md,
        SUM(IF(points = 1, 1, 0)) OVER(PARTITION BY team_id ORDER BY matchday ASC) AS draw_by_md,
        SUM(IF(points = 0, 1, 0)) OVER(PARTITION BY team_id ORDER BY matchday ASC) AS lose_by_md,
        SUM(points) OVER(PARTITION BY team_id ORDER BY matchday ASC) AS points_by_md,
        SUM(goal_scored) OVER(PARTITION BY team_id ORDER BY matchday ASC) AS gs_by_md,
        SUM(goal_against) OVER(PARTITION BY team_id ORDER BY matchday ASC) AS ga_by_md,
        (SUM(goal_scored) OVER(PARTITION BY team_id ORDER BY matchday ASC) - 
         SUM(goal_against) OVER(PARTITION BY team_id ORDER BY matchday ASC)) AS goal_diff_by_md
    FROM matchday_cte
)
SELECT 
*
FROM point_cte
ORDER BY league_id, club_name, matchday 