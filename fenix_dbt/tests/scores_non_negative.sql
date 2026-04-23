-- Custom test: all scores must be >= 0
SELECT match_id, home_score, away_score
FROM {{ ref('gold_match_results') }}
WHERE home_score < 0 OR away_score < 0
