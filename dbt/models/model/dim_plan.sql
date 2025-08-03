{{ config(
    materialized='table',
    unique_key='bene_id'
) }}

WITH monthly_struct AS (
    SELECT 
        DISTINCT
        bene_id,
        {% for i in range(1, 13) %}
        STRUCT(
            '{{ i }}' AS month,
            ptc_contract_id_{{ '{:02}'.format(i) }} AS ptc_contract,
            ptc_pbp_id_{{ '{:02}'.format(i) }} AS ptc_pbp,
            ptc_plan_code_{{ '{:02}'.format(i) }} AS ptc_plan,
            ptd_contract_id_{{ '{:02}'.format(i) }} AS ptd_contract,
            ptd_pbp_id_{{ '{:02}'.format(i) }} AS ptd_pbp,
            ptd_segment_id_{{ '{:02}'.format(i) }} AS ptd_segment
        ) AS m{{ i }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    FROM {{ ref('stg_coverage') }}
    WHERE bene_id IS NOT NULL
)

SELECT
    bene_id,
    SAFE_CAST(monthly_data.month AS INT) AS month,
    monthly_data.ptc_contract,
    monthly_data.ptc_pbp,
    monthly_data.ptc_plan,
    monthly_data.ptd_contract,
    monthly_data.ptd_pbp,
    monthly_data.ptd_segment
FROM monthly_struct,
UNNEST([
    {% for i in range(1, 13) %}m{{ i }}{% if not loop.last %}, {% endif %}
    {% endfor %}
]) AS monthly_data
WHERE bene_id IS NOT NULL
    AND monthly_data.ptc_contract IS NOT NULL
    AND monthly_data.ptc_pbp IS NOT NULL
    AND monthly_data.ptc_plan IS NOT NULL
    AND monthly_data.ptd_contract IS NOT NULL
    AND monthly_data.ptd_pbp IS NOT NULL
    AND monthly_data.ptd_segment IS NOT NULL