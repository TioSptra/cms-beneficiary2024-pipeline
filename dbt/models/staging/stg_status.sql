{{ config(
    materialized='table'
) }}

SELECT
    SAFE_CAST(BENE_ID AS INTEGER) AS bene_id,
    SAFE_CAST(ENTLMT_RSN_ORIG AS INTEGER) AS entitlement_reason_original,
    SAFE_CAST(ENTLMT_RSN_CURR AS INTEGER) AS entitlement_reason_current,
        CASE 
        WHEN ESRD_IND = 'Y' THEN 'Y'
        WHEN ESRD_IND = '0' THEN 'N'
        ELSE NULL
    END AS esrd_indicator,
    SAFE_CAST(BENE_PTA_TRMNTN_CD AS INTEGER) AS pta_termination_code,
    SAFE_CAST(BENE_PTB_TRMNTN_CD AS INTEGER) AS ptb_termination_code,


    -- Medicare status per bulan
    {% for i in range(1, 13) %}
    SAFE_CAST(MDCR_STATUS_CODE_{{ '{:02}'.format(i) }} AS INTEGER) AS mdcr_status_code_{{ '{:02}'.format(i) }},
    {% endfor %}

    SAFE_CAST(ENRL_SRC AS STRING) AS enrollment_source,

    CASE
        WHEN SAMPLE_GROUP IS NULL THEN 'unknown'
        ELSE SAFE_CAST(SAMPLE_GROUP AS STRING)
    END AS sample_group,
    
    CASE
        WHEN ENHANCED_FIVE_PERCENT_FLAG = ' ' THEN 'unknown'
        ELSE SAFE_CAST(ENHANCED_FIVE_PERCENT_FLAG AS STRING)
    END AS enhanced_five_percent_flag,
    SAFE_CAST(COVSTART AS DATE) AS covstart
FROM `purwadika.jcdeol005_capstone3_tio_raw.raw_beneficiary_2024`
