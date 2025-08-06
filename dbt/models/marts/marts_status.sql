{{ config(
    materialized = 'table'
) }}

SELECT
    year,
    month,
    mdcr_status,
    COUNT(DISTINCT bene_id) AS total_beneficiaries

FROM {{ ref('fact_beneficiary_month') }} 

WHERE mdcr_status IS NOT NULL

GROUP BY
    year, month, mdcr_status

ORDER BY
    year, month, mdcr_status
