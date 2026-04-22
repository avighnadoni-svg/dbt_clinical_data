{{ config(materialized='table') }}

with src as (
    select * from {{ source('bronze', 'br_imaging_studies') }}
),

enc as (
    select encounter_key, encounter_id
    from {{ ref('fct_encounter') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['src.ID']) }} as imaging_study_fact_key,
        src.ID as imaging_study_id,
        p.patient_key,
        e.encounter_key,
        d.date_key as imaging_date_key,
        bs.clinical_code_key as body_site_code_key,
        md.clinical_code_key as modality_code_key,
        sop.clinical_code_key as sop_code_key,
        src.DATE as imaging_ts,
        1 as imaging_study_count,
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
    left join {{ ref('dim_clinical_code') }} bs
        on src.BODYSITE_CODE = bs.code
       and bs.code_type = 'imaging_body_site'
    left join {{ ref('dim_clinical_code') }} md
        on src.MODALITY_CODE = md.code
       and md.code_type = 'imaging_modality'
    left join {{ ref('dim_clinical_code') }} sop
        on src.SOP_CODE = sop.code
       and sop.code_type = 'imaging_sop'
)

select * from final