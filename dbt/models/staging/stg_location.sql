{{ config(
    materialized='table'
) }}

SELECT
    SAFE_CAST(BENE_ID AS integer) AS bene_id,
    SAFE_CAST(STATE_CODE AS integer) AS state_code,
    SAFE_CAST(COUNTY_CD AS integer) AS county_cd,
    SAFE_CAST(ZIP_CD AS INTEGER) AS zip_code,

    {% for i in range(1,13) %}
    SAFE_CAST(STATE_CNTY_FIPS_CD_{{ '{:02}'.format(i) }} AS integer) AS state_cnty_fips_cd_{{ '{:02}'.format(i) }}{% if not loop.last %},{% endif %}
    {% endfor %}

FROM `purwadika.jcdeol005_capstone3_tio_raw.raw_beneficiary_2024`

WHERE
    BENE_ID IS NOT NULL
    AND ZIP_CD IS NOT NULL
    AND SAFE_CAST(ZIP_CD AS INTEGER) > 0
    {% for i in range(1,13) %}
    AND STATE_CNTY_FIPS_CD_{{ '{:02}'.format(i) }} IS NOT NULL
    {% endfor %}
