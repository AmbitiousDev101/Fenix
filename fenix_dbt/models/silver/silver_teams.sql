{{ config(materialized='view') }}

WITH home_teams AS (
    SELECT DISTINCT home_team_id AS team_id, home_team_name AS team_name, competition_id
    FROM {{ ref('silver_matches') }}
    WHERE home_team_id IS NOT NULL
),
away_teams AS (
    SELECT DISTINCT away_team_id AS team_id, away_team_name AS team_name, competition_id
    FROM {{ ref('silver_matches') }}
    WHERE away_team_id IS NOT NULL
)
SELECT DISTINCT team_id, team_name, competition_id
FROM (
    SELECT * FROM home_teams
    UNION
    SELECT * FROM away_teams
)
