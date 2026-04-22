{{ config(materialized='table') }}

with encounter_codes as (
    select
        CODE as code,
        DESCRIPTION as description,
        'encounter' as code_type,
        'encounter' as clinical_domain
    from {{ source('bronze', 'br_encounters') }}
    where CODE is not null
),

condition_codes as (
    select
        CODE as code,
        DESCRIPTION as description,
        'condition' as code_type,
        'diagnosis' as clinical_domain
    from {{ source('bronze', 'br_conditions') }}
    where CODE is not null
),

observation_codes as (
    select
        CODE as code,
        DESCRIPTION as description,
        'observation' as code_type,
        case
            when lower(TYPE) like '%numeric%' then 'lab_or_measurement'
            else 'clinical_observation'
        end as clinical_domain
    from {{ source('bronze', 'br_observations') }}
    where CODE is not null
),

medication_codes as (
    select
        CODE as code,
        DESCRIPTION as description,
        'medication' as code_type,
        'treatment' as clinical_domain
    from {{ source('bronze', 'br_medications') }}
    where CODE is not null
),

procedure_codes as (
    select
        CODE as code,
        DESCRIPTION as description,
        'procedure' as code_type,
        'procedure' as clinical_domain
    from {{ source('bronze', 'br_procedures') }}
    where CODE is not null
),

careplan_codes as (
    select
        CODE as code,
        DESCRIPTION as description,
        'careplan' as code_type,
        'careplan' as clinical_domain
    from {{ source('bronze', 'br_careplans') }}
    where CODE is not null
),

device_codes as (
    select
        CODE as code,
        DESCRIPTION as description,
        'device' as code_type,
        'device' as clinical_domain
    from {{ source('bronze', 'br_devices') }}
    where CODE is not null
),

immunization_codes as (
    select
        CODE as code,
        DESCRIPTION as description,
        'immunization' as code_type,
        'preventive' as clinical_domain
    from {{ source('bronze', 'br_immunizations') }}
    where CODE is not null
),

supply_codes as (
    select
        CODE as code,
        DESCRIPTION as description,
        'supply' as code_type,
        'operational' as clinical_domain
    from {{ source('bronze', 'br_supplies') }}
    where CODE is not null
),

allergy_codes as (
    select
        CODE as code,
        DESCRIPTION as description,
        'allergy' as code_type,
        'allergy' as clinical_domain
    from {{ source('bronze', 'br_allergies') }}
    where CODE is not null
),

imaging_body_site_codes as (
    select
        BODYSITE_CODE as code,
        BODYSITE_DESCRIPTION as description,
        'imaging_body_site' as code_type,
        'imaging' as clinical_domain
    from {{ source('bronze', 'br_imaging_studies') }}
    where BODYSITE_CODE is not null
),

imaging_modality_codes as (
    select
        MODALITY_CODE as code,
        MODALITY_DESCRIPTION as description,
        'imaging_modality' as code_type,
        'imaging' as clinical_domain
    from {{ source('bronze', 'br_imaging_studies') }}
    where MODALITY_CODE is not null
),

imaging_sop_codes as (
    select
        SOP_CODE as code,
        SOP_DESCRIPTION as description,
        'imaging_sop' as code_type,
        'imaging' as clinical_domain
    from {{ source('bronze', 'br_imaging_studies') }}
    where SOP_CODE is not null
),

all_codes as (
    select * from encounter_codes
    union all
    select * from condition_codes
    union all
    select * from observation_codes
    union all
    select * from medication_codes
    union all
    select * from procedure_codes
    union all
    select * from careplan_codes
    union all
    select * from device_codes
    union all
    select * from immunization_codes
    union all
    select * from supply_codes
    union all
    select * from allergy_codes
    union all
    select * from imaging_body_site_codes
    union all
    select * from imaging_modality_codes
    union all
    select * from imaging_sop_codes
),

deduped as (
    select
        code,
        description,
        code_type,
        clinical_domain,
        row_number() over (
            partition by code, code_type
            order by description
        ) as rn
    from all_codes
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['code','code_type']) }} as clinical_code_key,
        code,
        description,
        code_type,
        clinical_domain,
        1 as is_active
    from deduped
    where rn = 1
)

select * from final