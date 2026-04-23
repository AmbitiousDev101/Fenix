{{ config(materialized='view') }}

SELECT *
FROM iceberg_scan(
    '{{ var("warehouse_path") }}/silver/matches',
    allow_moved_paths = true
)
