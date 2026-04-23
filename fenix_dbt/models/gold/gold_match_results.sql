{{ config(materialized='table') }}

SELECT match_id, competition_id, utc_date, matchday,
       home_team_id, home_team_name, away_team_id, away_team_name,
       home_score, away_score,
       CASE WHEN home_score > away_score THEN 'home_win'
            WHEN home_score = away_score THEN 'draw'
            ELSE 'away_win' END AS result
FROM {{ ref('silver_matches') }}
WHERE status = 'finished' AND home_score IS NOT NULL AND away_score IS NOT NULL
