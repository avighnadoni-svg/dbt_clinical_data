{{ config(materialized='table') }}

with src as (
    select * from {{ source('bronze', 'br_supplies') }}
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
            'src.DATE'
        ]) }} as supply_fact_key,
        p.patient_key,
        e.encounter_key,
        d.date_key as supply_date_key,
        cc.clinical_code_key as supply_code_key,
        src.DATE as supply_ts,
        1 as supply_event_count,
        src.QUANTITY as quantity,
        src.LOAD_TS,
        src.SOURCE_FILE,
        src.BATCH_ID
    from src
    left join {{ ref('dim_patient') }} p
        on src.PATIENT = p.patient_id
    left join enc e
        on src.ENCOUNTER = e.encounter_id
    left join {{ ref('dim_date') }} d
        on cast(src.DATE as date) = d.full_date
    left join {{ ref('dim_clinical_code') }} cc
        on src.CODE = cc.code
       and cc.code_type = 'supply'
)

select * from final