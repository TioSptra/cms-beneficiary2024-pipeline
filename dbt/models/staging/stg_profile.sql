{{ config(
    materialized='table'
) }}

SELECT
    SAFE_CAST(BENE_ID AS INTEGER) AS bene_id,
    SAFE_CAST(BENE_BIRTH_DT AS DATE) AS birth_date,
    CASE 
        WHEN SEX_IDENT_CD = 1 THEN 'Male'
        WHEN SEX_IDENT_CD = 2 THEN 'Female'
        ELSE NULL
    END AS gender,
    SAFE_CAST(BENE_RACE_CD AS INTEGER) AS race_code,
    CASE 
        WHEN BENE_DEATH_DT IS NULL THEN DATE '9999-12-31'
        ELSE SAFE_CAST(BENE_DEATH_DT AS DATE)
    END AS death_date,
    SAFE_CAST(BENE_ENROLLMT_REF_YR AS INTEGER) AS enrollment_reference_year,
    SAFE_CAST(AGE_AT_END_REF_YR AS INTEGER) AS age_at_end_of_year,
    SAFE_CAST(RTI_RACE_CD AS integer) AS rti_race_code,
    CASE 
        WHEN CRNT_BIC_CD = '10' THEN NULL
        ELSE SAFE_CAST(CRNT_BIC_CD AS STRING)
        END AS current_bic_code,
    CASE 
        WHEN VALID_DEATH_DT_SW IS NULL THEN 'N'
        ELSE SAFE_CAST(VALID_DEATH_DT_SW as STRING)
    END AS valid_death_date_flag
FROM `purwadika.jcdeol005_capstone3_tio_raw.raw_beneficiary_2024`
WHERE
    bene_id IS NOT NULL
    AND ZIP_CD IS NOT NULL
    AND SAFE_CAST(ZIP_CD AS INTEGER) > 0
    AND CRNT_BIC_CD != '10'