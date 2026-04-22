{{ config(materialized='table') }}

with src as (
    select * from {{ source('bronze', 'br_observations') }}
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
            'src.DATE',
            'src.VALUE'
        ]) }} as observation_fact_key,
        p.patient_key,
        e.encounter_key,
        d.date_key as observation_date_key,
        cc.clinical_code_key as observation_code_key,
        src.DATE as observation_ts,
        1 as observation_event_count,
        try_to_number(src.VALUE) as observation_value_num,
        src.VALUE as observation_value_raw,
        src.UNITS as units,
        src.TYPE as observation_type,
        case when try_to_number(src.VALUE) is not null then 1 else 0 end as is_numeric_flag,
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
       and cc.code_type = 'observation'
)

select * from final