{{ config(
    materialized='table'
) }}

SELECT 
    gender,
    CASE 
        WHEN age_at_end_of_year < 5 THEN 'Toddler'
        WHEN age_at_end_of_year BETWEEN 5 AND 9 THEN 'Child'
        WHEN age_at_end_of_year BETWEEN 10 AND 18 THEN 'Teenager'
        WHEN age_at_end_of_year BETWEEN 19 AND 60 THEN 'Adult'
        WHEN age_at_end_of_year > 60 THEN 'Elderly'
        ELSE 'Unknown'
    END AS age_group,
    COUNT(*) AS total_beneficiaries

FROM {{ ref('dim_profile') }}
GROUP BY gender,
        CASE 
            WHEN age_at_end_of_year < 5 THEN 'Toddler'
            WHEN age_at_end_of_year BETWEEN 5 AND 9 THEN 'Child'
            WHEN age_at_end_of_year BETWEEN 10 AND 18 THEN 'Teenager'
            WHEN age_at_end_of_year BETWEEN 19 AND 60 THEN 'Adult'
            WHEN age_at_end_of_year > 60 THEN 'Elderly'
            ELSE 'Unknown'
        END
ORDER BY gender, age_group
