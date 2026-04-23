{{ config(materialized='table') }}

WITH match_explode AS (
    SELECT match_id, competition_id, utc_date,
           home_team_id AS team_id, home_team_name AS team_name,
           away_team_id AS opponent_id, away_team_name AS opponent_name,
           'home' AS venue, home_score AS goals_scored, away_score AS goals_conceded
    FROM {{ ref('silver_matches') }}
    WHERE status = 'finished' AND home_score IS NOT NULL

    UNION ALL

    SELECT match_id, competition_id, utc_date,
           away_team_id AS team_id, away_team_name AS team_name,
           home_team_id AS opponent_id, home_team_name AS opponent_name,
           'away' AS venue, away_score AS goals_scored, home_score AS goals_conceded
    FROM {{ ref('silver_matches') }}
    WHERE status = 'finished' AND away_score IS NOT NULL
),
with_results AS (
    SELECT *,
        CASE WHEN goals_scored > goals_conceded THEN 'W'
             WHEN goals_scored = goals_conceded THEN 'D'
             ELSE 'L' END AS result,
        CASE WHEN goals_scored > goals_conceded THEN 3
             WHEN goals_scored = goals_conceded THEN 1
             ELSE 0 END AS points
    FROM match_explode
),
with_rolling AS (
    SELECT *,
        ROUND(AVG(goals_scored) OVER w5, 2) AS goals_scored_rolling_avg_5,
        ROUND(AVG(goals_conceded) OVER w5, 2) AS goals_conceded_rolling_avg_5,
        SUM(points) OVER w5 AS points_last5
    FROM with_results
    WINDOW w5 AS (PARTITION BY team_id ORDER BY utc_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
)
SELECT *,
    CASE
        WHEN LAG(points_last5, 5) OVER (PARTITION BY team_id ORDER BY utc_date) IS NULL THEN 'improving'
        WHEN points_last5 > LAG(points_last5, 5) OVER (PARTITION BY team_id ORDER BY utc_date) THEN 'improving'
        ELSE 'declining'
    END AS form_trend
FROM with_rolling
