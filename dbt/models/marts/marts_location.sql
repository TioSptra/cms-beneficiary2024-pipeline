{{ config(
    materialized = 'table'
) }}

SELECT
    l.state_code as state,
    l.county_code as county,
    COUNT(DISTINCT f.bene_id) AS total_beneficiaries

FROM {{ ref('fact_beneficiary_month') }} f
JOIN {{ ref('dim_location') }} l
    ON f.bene_id = l.bene_id

GROUP BY
    l.state_code, l.county_code

ORDER BY
    total_beneficiaries DESC
