{{ config(
    materialized='table',
    unique_key='bene_id'
) }}

SELECT
    DISTINCT
    p.bene_id,
    p.enrollment_reference_year AS enrollment_year,
    s.enrollment_source,
    s.sample_group,
    s.enhanced_five_percent_flag,
    s.covstart

FROM {{ ref('stg_profile')}} p
LEFT JOIN {{ ref('stg_status')}} s
    on p.bene_id = s.bene_id
WHERE p.bene_id IS NOT NULL
