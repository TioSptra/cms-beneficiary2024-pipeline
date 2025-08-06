{{ config(
    materialized='table',
    unique_key='bene_id'
) }}

SELECT
    bene_id,
    gender,
    birth_date,
    death_date,
    age_at_end_of_year,
    rti_race_code as race_final,
    CASE rti_race_code
        WHEN 1 THEN 'White'
        WHEN 2 THEN 'Black'
        WHEN 3 THEN 'Other'
        WHEN 4 THEN 'Asian'
        WHEN 5 THEN 'Hispanic'
        WHEN 6 THEN 'American Indian or Alaska Native'
        ELSE 'Unknown'
    END AS race_desc,
    current_bic_code,
    valid_death_date_flag

FROM {{ ref('stg_profile') }} 
WHERE bene_id IS NOT NULL
