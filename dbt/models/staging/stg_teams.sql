SELECT
*
FROM
{{ source('raw_data', 'raw_teams') }}
limit 5