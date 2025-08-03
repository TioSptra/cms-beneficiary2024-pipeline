{{ config(
    materialized='table',
    unique_key='bene_id'
) }}

WITH monthly_struct AS (
SELECT
    DISTINCT
    bene_id,
    county_cd as county_code,
    zip_code,
    state_code,

    {% for i in range(1, 13) %}
    STRUCT(
    '{{ i }}' AS month,
    state_cnty_fips_cd_{{ '{:02}'.format(i) }} AS state_county_fips
    ) AS m{{ i }}{% if not loop.last %}, {% endif %}
    {% endfor %}

FROM {{ ref('stg_location') }}
WHERE bene_id IS NOT NULL
)

SELECT
    bene_id,
    SAFE_CAST(monthly_data.month AS INT) AS month,
    county_code,
    zip_code,
    state_code,
    monthly_data.state_county_fips

FROM monthly_struct,
UNNEST([
    {% for i in range(1, 13) %}m{{ i }}{% if not loop.last %}, {% endif %}
    {% endfor %}
]) AS monthly_data
WHERE bene_id IS NOT NULL
AND monthly_data.state_county_fips IS NOT NULL