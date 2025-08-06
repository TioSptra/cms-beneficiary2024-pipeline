{{ config(
    materialized='table',
    unique_key='bene_id'
) }}

SELECT
    DISTINCT
    bene_id,
    entitlement_reason_original,
    entitlement_reason_current,
    esrd_indicator,
    pta_termination_code,
    ptb_termination_code

FROM {{ ref('stg_status')}} 
WHERE bene_id IS NOT NULL
