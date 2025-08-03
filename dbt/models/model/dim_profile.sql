{{ config(
    materialized='table',
    unique_key='bene_id'
) }}

SELECT
    DISTINCT
    p.bene_id,
    p.gender,
    p.birth_date,
    p.death_date,
    p.age_at_end_of_year,
    p.rti_race_code as race_final,
    CASE p.rti_race_code
        WHEN 1 THEN 'White'
        WHEN 2 THEN 'Black'
        WHEN 3 THEN 'Other'
        WHEN 4 THEN 'Asian'
        WHEN 5 THEN 'Hispanic'
        WHEN 6 THEN 'American Indian or Alaska Native'
        ELSE 'Unknown'
    END AS race_desc,
    p.current_bic_code,
    l.entitlement_reason_original,
    l.entitlement_reason_current,
    l.esrd_indicator,
    l.pta_termination_code,
    l.ptb_termination_code,
    p.valid_death_date_flag,
    CURRENT_DATE() AS valid_from,
    DATE '9999-12-31' AS valid_to,
    TRUE AS is_current

FROM {{ ref('stg_profile') }} p
LEFT JOIN {{ref ('stg_status') }} l
    on p.bene_id = l.bene_id
WHERE p.bene_id IS NOT NULL
