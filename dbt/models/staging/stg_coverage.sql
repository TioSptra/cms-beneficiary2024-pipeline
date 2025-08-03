{{ config(
    materialized='table'
) }}

SELECT
    SAFE_CAST(BENE_ID AS INTEGER) AS bene_id,
    SAFE_CAST(BENE_HI_CVRAGE_TOT_MONS AS INTEGER) AS hi_coverage_total_months,
    SAFE_CAST(BENE_SMI_CVRAGE_TOT_MONS AS INTEGER) AS smi_coverage_total_months,
    SAFE_CAST(BENE_STATE_BUYIN_TOT_MONS AS INTEGER) AS state_buyin_total_months,
    SAFE_CAST(BENE_HMO_CVRAGE_TOT_MONS AS INTEGER) AS hmo_coverage_total_months,
    SAFE_CAST(PTD_PLAN_CVRG_MONS AS INTEGER) AS ptd_plan_coverage_months,
    SAFE_CAST(RDS_CVRG_MONS AS INTEGER) AS rds_coverage_months,
    SAFE_CAST(DUAL_ELGBL_MONS AS INTEGER) AS dual_eligible_months,

    -- Per bulan indikator Buy-in
    {% for i in range(1,13) %}
    CASE 
        WHEN MDCR_ENTLMT_BUYIN_IND_{{ '{:02}'.format(i) }} IN ('1', 'C') THEN 'Y'
        WHEN MDCR_ENTLMT_BUYIN_IND_{{ '{:02}'.format(i) }} = '3' THEN 'N'
        ELSE NULL
    END AS mdcr_buyin_ind_{{ '{:02}'.format(i) }},
    {% endfor %}

    -- Per bulan indikator HMO
    {% for i in range(1,13) %}
    CASE
        WHEN HMO_IND_{{ '{:02}'.format(i) }} IS NULL THEN 'N'
        ELSE SAFE_CAST(HMO_IND_{{ '{:02}'.format(i) }} AS STRING)
    END AS hmo_ind_{{ '{:02}'.format(i) }},
    {% endfor %}


    {% for i in range(1,13) %}
    SAFE_CAST(PTC_CNTRCT_ID_{{ '{:02}'.format(i) }} AS STRING) AS ptc_contract_id_{{ '{:02}'.format(i) }},
    {% endfor %}

    {% for i in range(1,13) %}
    SAFE_CAST(PTC_PBP_ID_{{ '{:02}'.format(i) }} AS INTEGER) AS ptc_pbp_id_{{ '{:02}'.format(i) }},
    {% endfor %}

    {% for i in range(1,13) %}
    CASE
        WHEN PTC_PLAN_TYPE_CD_{{ '{:02}'.format(i) }} IS NULL THEN 'N'
        ELSE SAFE_CAST(PTC_PLAN_TYPE_CD_{{ '{:02}'.format(i) }} AS STRING)
    END AS ptc_plan_code_{{ '{:02}'.format(i) }},
    {% endfor %}

    -- Per bulan Part D Contract ID
    {% for i in range(1,13) %}
    SAFE_CAST(PTD_CNTRCT_ID_{{ '{:02}'.format(i) }} AS STRING) AS ptd_contract_id_{{ '{:02}'.format(i) }},
    {% endfor %}

    {% for i in range(1,13) %}
    SAFE_CAST(PTD_PBP_ID_{{ '{:02}'.format(i) }} AS INTEGER) AS ptd_pbp_id_{{ '{:02}'.format(i) }},
    {% endfor %}

    {% for i in range(1,13) %}
    CASE
        WHEN PTD_SGMT_ID_{{ '{:02}'.format(i) }} IS NULL THEN 0
        ELSE SAFE_CAST(PTD_SGMT_ID_{{ '{:02}'.format(i) }} AS INTEGER)
    END AS ptd_segment_id_{{ '{:02}'.format(i) }},
    {% endfor %}

    -- Per bulan RDS indicator
    {% for i in range(1,13) %}
    CASE 
        WHEN CAST(RDS_IND_{{ '{:02}'.format(i) }} AS STRING) IN ('0', 'N', 'false') THEN 'N'
        WHEN CAST(RDS_IND_{{ '{:02}'.format(i) }} AS STRING) IN ('Y', 'true') THEN 'Y'
        ELSE NULL
        END AS rds_ind_{{ '{:02}'.format(i) }},
    {% endfor %}


    -- Per bulan dual status code
    {% for i in range(1,13) %}
        CASE
            WHEN DUAL_STUS_CD_{{ '{:02}'.format(i) }} IN ('NA','AA') OR DUAL_STUS_CD_{{ '{:02}'.format(i) }} IS NULL THEN '00'
            ELSE SAFE_CAST(DUAL_STUS_CD_{{ '{:02}'.format(i) }} AS STRING)
        END AS dual_status_code_{{ '{:02}'.format(i) }},
    {% endfor %}

    -- Per bulan cost share group code
    {% for i in range(1,13) %}
    SAFE_CAST(CST_SHR_GRP_CD_{{ '{:02}'.format(i) }} AS STRING) AS cost_share_group_code_{{ '{:02}'.format(i) }}
    {% if not loop.last %},{% endif %}
    {% endfor %}

FROM `purwadika.jcdeol005_capstone3_tio_raw.raw_beneficiary_2024`
WHERE
    BENE_ID IS NOT NULL

    {% for i in range(1,13) %}
        AND PTD_CNTRCT_ID_{{ '{:02}'.format(i) }} IS NOT NULL
    {% endfor %}

        {% for i in range(1,13) %}
        AND PTC_CNTRCT_ID_{{ '{:02}'.format(i) }} IS NOT NULL
    {% endfor %}

        {% for i in range(1,13) %}
        AND PTD_PBP_ID_{{ '{:02}'.format(i) }} IS NOT NULL
    {% endfor %}
    
        {% for i in range(1,13) %}
        AND PTC_PBP_ID_{{ '{:02}'.format(i) }} IS NOT NULL
    {% endfor %}


