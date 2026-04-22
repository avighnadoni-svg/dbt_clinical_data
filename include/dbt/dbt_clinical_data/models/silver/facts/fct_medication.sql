{{ config(materialized='table') }}

with src as (
    select * from {{ source('bronze', 'br_medications') }}
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
        ]) }} as medication_fact_key,
        p.patient_key,
        e.encounter_key,
        py.payer_key,
        cc.clinical_code_key as medication_code_key,
        rs.clinical_code_key as reason_code_key,
        ds.date_key as medication_start_date_key,
        de.date_key as medication_stop_date_key,
        src."START" as medication_start_ts,
        src."STOP" as medication_stop_ts,
        1 as medication_event_count,
        src.BASE_COST as base_cost,
        src.PAYER_COVERAGE as payer_coverage,
        src.DISPENSES as dispenses,
        src.TOTALCOST as total_cost,
        datediff('day', src."START", src."STOP") as medication_duration_days,
        src.LOAD_TS,
        src.SOURCE_FILE,
        src.BATCH_ID
    from src
    left join {{ ref('dim_patient') }} p
        on src.PATIENT = p.patient_id
    left join enc e
        on src.ENCOUNTER = e.encounter_id
    left join {{ ref('dim_payer') }} py
        on src.PAYER = py.payer_id
    left join {{ ref('dim_clinical_code') }} cc
        on src.CODE = cc.code
       and cc.code_type = 'medication'
    left join {{ ref('dim_clinical_code') }} rs
        on src.REASONCODE = rs.code
    left join {{ ref('dim_date') }} ds
        on cast(src."START" as date) = ds.full_date
    left join {{ ref('dim_date') }} de
        on cast(src."STOP" as date) = de.full_date
)

select * from final