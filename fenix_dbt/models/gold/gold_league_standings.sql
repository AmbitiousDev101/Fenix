{{ config(materialized='table') }}

SELECT team_id, team_name,
       COUNT(*) AS played,
       SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS won,
       SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) AS drawn,
       SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS lost,
       SUM(points) AS total_points,
       SUM(goals_scored) AS goals_for,
       SUM(goals_conceded) AS goals_against,
       SUM(goals_scored) - SUM(goals_conceded) AS goal_difference,
       ROW_NUMBER() OVER (
           ORDER BY SUM(points) DESC,
                    SUM(goals_scored) - SUM(goals_conceded) DESC,
                    SUM(goals_scored) DESC
       ) AS position
FROM {{ ref('gold_team_form') }}
GROUP BY team_id, team_name
ORDER BY position
