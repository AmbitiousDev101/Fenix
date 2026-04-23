-- Custom test: points_last5 must be between 0 and 15 (max 5 wins × 3 pts)
SELECT team_id, match_id, points_last5
FROM {{ ref('gold_team_form') }}
WHERE points_last5 < 0 OR points_last5 > 15
