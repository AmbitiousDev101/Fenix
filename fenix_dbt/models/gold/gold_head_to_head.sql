{{ config(materialized='table') }}

WITH matches AS (
    SELECT
        LEAST(home_team_id, away_team_id) AS team_a_id,
        CASE WHEN home_team_id < away_team_id THEN home_team_name ELSE away_team_name END AS team_a_name,
        GREATEST(home_team_id, away_team_id) AS team_b_id,
        CASE WHEN home_team_id < away_team_id THEN away_team_name ELSE home_team_name END AS team_b_name,
        home_score, away_score, home_team_id,
        CASE WHEN home_score > away_score THEN home_team_id
             WHEN away_score > home_score THEN away_team_id
             ELSE NULL END AS winner_id
    FROM {{ ref('gold_match_results') }}
)
SELECT team_a_id, team_a_name, team_b_id, team_b_name,
       COUNT(*) AS total_matches,
       SUM(CASE WHEN winner_id = team_a_id THEN 1 ELSE 0 END) AS team_a_wins,
       SUM(CASE WHEN winner_id = team_b_id THEN 1 ELSE 0 END) AS team_b_wins,
       SUM(CASE WHEN winner_id IS NULL THEN 1 ELSE 0 END) AS draws,
       ROUND(AVG(home_score + away_score), 2) AS avg_total_goals
FROM matches
GROUP BY team_a_id, team_a_name, team_b_id, team_b_name
