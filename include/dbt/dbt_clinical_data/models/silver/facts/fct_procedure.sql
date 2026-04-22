{{ config(materialized='table') }}

with src as (
    select * from {{ source('bronze', 'br_procedures') }}
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
        ]) }} as procedure_fact_key,
        p.patient_key,
        e.encounter_key,
        d.date_key as procedure_date_key,
        cc.clinical_code_key as procedure_code_key,
        rs.clinical_code_key as reason_code_key,
        src.DATE as procedure_ts,
        1 as procedure_event_count,
        src.BASE_COST as base_cost,
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
       and cc.code_type = 'procedure'
    left join {{ ref('dim_clinical_code') }} rs
        on src.REASONCODE = rs.code
)

select * from final