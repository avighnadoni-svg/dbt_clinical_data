{{ config(materialized='table') }}

with disease_base as (
    select
        fc.condition_code_key,
        fc.patient_key,
        fc.encounter_key,
        fc.condition_event_count,
        fc.is_active_flag,
        fc.condition_start_date_key
    from {{ ref('fct_condition') }} fc
),

encounter_cost as (
    select
        encounter_key,
        total_claim_cost,
        payer_coverage,
        uncovered_amount
    from {{ ref('fct_encounter') }}
)

select
    cc.clinical_code_key as disease_code_key,
    cc.code as disease_code,
    cc.description as disease_description,
    cc.clinical_domain,

    count(*) as diagnosis_event_count,
    count(distinct db.patient_key) as patient_count,
    count(distinct db.encounter_key) as encounter_count,
    sum(db.is_active_flag) as active_diagnosis_count,

    sum(coalesce(e.total_claim_cost, 0)) as total_claim_cost,
    sum(coalesce(e.payer_coverage, 0)) as total_payer_coverage,
    sum(coalesce(e.uncovered_amount, 0)) as total_uncovered_amount

from disease_base db
left join {{ ref('dim_clinical_code') }} cc
    on db.condition_code_key = cc.clinical_code_key
left join encounter_cost e
    on db.encounter_key = e.encounter_key
group by
    cc.clinical_code_key,
    cc.code,
    cc.description,
    cc.clinical_domain