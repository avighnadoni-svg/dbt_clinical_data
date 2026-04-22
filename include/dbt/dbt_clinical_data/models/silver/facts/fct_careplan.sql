{{ config(materialized='table') }}

with src as (
    select * from {{ source('bronze', 'br_careplans') }}
),

enc as (
    select encounter_key, encounter_id
    from {{ ref('fct_encounter') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['src.ID']) }} as careplan_fact_key,
        src.ID as careplan_id,
        p.patient_key,
        e.encounter_key,
        cc.clinical_code_key as careplan_code_key,
        rs.clinical_code_key as reason_code_key,
        ds.date_key as careplan_start_date_key,
        de.date_key as careplan_stop_date_key,
        src."START" as careplan_start_ts,
        src."STOP" as careplan_stop_ts,
        1 as careplan_event_count,
        datediff('day', src."START", src."STOP") as careplan_duration_days,
        case when src."STOP" is null then 1 else 0 end as is_active_flag,
        src.LOAD_TS,
        src.SOURCE_FILE,
        src.BATCH_ID
    from src
    left join {{ ref('dim_patient') }} p
        on src.PATIENT = p.patient_id
    left join enc e
        on src.ENCOUNTER = e.encounter_id
    left join {{ ref('dim_clinical_code') }} cc
        on src.CODE = cc.code
       and cc.code_type = 'careplan'
    left join {{ ref('dim_clinical_code') }} rs
        on src.REASONCODE = rs.code
    left join {{ ref('dim_date') }} ds
        on cast(src."START" as date) = ds.full_date
    left join {{ ref('dim_date') }} de
        on cast(src."STOP" as date) = de.full_date
)

select * from final