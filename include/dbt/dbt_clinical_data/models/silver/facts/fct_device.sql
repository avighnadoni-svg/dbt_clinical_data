{{ config(materialized='table') }}

with src as (
    select * from {{ source('bronze', 'br_devices') }}
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
            'src."START"',
            'src.UDI'
        ]) }} as device_fact_key,
        p.patient_key,
        e.encounter_key,
        cc.clinical_code_key as device_code_key,
        ds.date_key as device_start_date_key,
        de.date_key as device_stop_date_key,
        src."START" as device_start_ts,
        src."STOP" as device_stop_ts,
        src.UDI as udi,
        1 as device_event_count,
        datediff('day', src."START", src."STOP") as device_duration_days,
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
       and cc.code_type = 'device'
    left join {{ ref('dim_date') }} ds
        on cast(src."START" as date) = ds.full_date
    left join {{ ref('dim_date') }} de
        on cast(src."STOP" as date) = de.full_date
)

select * from final