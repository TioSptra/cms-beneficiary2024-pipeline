{{ config(
    materialized = 'table'
) }}

WITH monthly_struct AS (
    SELECT
        DISTINCT
        p.bene_id,
        p.enrollment_reference_year AS year,

        {% for i in range(1, 13) %}
        STRUCT(
            '{{ i }}' AS month,
            s.mdcr_status_code_{{ '{:02}'.format(i) }} AS mdcr_status,
            c.mdcr_buyin_ind_{{ '{:02}'.format(i) }} AS entitlement_buyin,
            c.dual_status_code_{{ '{:02}'.format(i) }} AS dual_status,
            c.cost_share_group_code_{{ '{:02}'.format(i) }} AS cost_sharing,
            c.hmo_ind_{{ '{:02}'.format(i) }} AS hmo_ind
        ) AS m{{ i }}{% if not loop.last %}, {% endif %}
        {% endfor %},

        -- Yearly indicators
        c.hi_coverage_total_months AS hi_coverage_mons,
        c.smi_coverage_total_months AS smi_coverage_mons,
        c.state_buyin_total_months AS state_buyin_mons,
        c.hmo_coverage_total_months AS hmo_coverage_mons,
        c.ptd_plan_coverage_months AS plan_coverage_mons,
        c.rds_coverage_months AS rds_coverage_mons,
        c.dual_eligible_months AS dual_eligible_mons

FROM {{ ref('stg_profile') }} p
LEFT JOIN {{ref ('stg_coverage') }} c
    on p.bene_id = c.bene_id
LEFT JOIN {{ref ('stg_status') }} s
    on p.bene_id = s.bene_id
WHERE p.bene_id IS NOT NULL

)

SELECT
    bene_id,
    SAFE_CAST(monthly_data.month AS INTEGER) AS month,
    year,
    monthly_data.mdcr_status,
    monthly_data.entitlement_buyin,
    monthly_data.dual_status,
    monthly_data.cost_sharing,
    monthly_data.hmo_ind,

    -- Repeated for all months
    hi_coverage_mons,
    smi_coverage_mons,
    state_buyin_mons,
    hmo_coverage_mons,
    plan_coverage_mons,
    rds_coverage_mons,
    dual_eligible_mons

FROM monthly_struct,
UNNEST([
    {% for i in range(1, 13) %}m{{ i }}{% if not loop.last %}, {% endif %}
    {% endfor %}
]) AS monthly_data
WHERE monthly_data.mdcr_status IS NOT NULL
    AND monthly_data.entitlement_buyin IS NOT NULL
    AND monthly_data.dual_status IS NOT NULL
    AND monthly_data.cost_sharing IS NOT NULL
    AND monthly_data.hmo_ind IS NOT NULL
    AND hi_coverage_mons IS NOT NULL
    AND smi_coverage_mons IS NOT NULL
    AND state_buyin_mons IS NOT NULL
    AND hmo_coverage_mons IS NOT NULL
    AND plan_coverage_mons IS NOT NULL
    AND rds_coverage_mons IS NOT NULL
    AND dual_eligible_mons IS NOT NULL
