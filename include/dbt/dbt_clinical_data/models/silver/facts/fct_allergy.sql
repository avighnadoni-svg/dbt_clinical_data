{{ config(materialized='table') }}

with src as (
    select * from {{ source('bronze', 'br_allergies') }}
),

enc as (
    select encounter_key, encounter_id
    from {{ ref('fct_encounter') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'src.PATIENT',
            'src.ENCOUNTER',
            'src.CODE',
            'src."START"'
        ]) }} as allergy_fact_key,
        p.patient_key,
        e.encounter_key,
        cc.clinical_code_key as allergy_code_key,
        ds.date_key as allergy_start_date_key,
        de.date_key as allergy_stop_date_key,
        src."START" as allergy_start_ts,
        src."STOP" as allergy_stop_ts,
        1 as allergy_event_count,
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
       and cc.code_type = 'allergy'
    left join {{ ref('dim_date') }} ds
        on cast(src."START" as date) = ds.full_date
    left join {{ ref('dim_date') }} de
        on cast(src."STOP" as date) = de.full_date
)

select * from final