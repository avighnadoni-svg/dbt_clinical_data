{{ config(materialized='table') }}

with encounter_agg as (
    select
        provider_key,
        count(*) as total_encounters,
        count(distinct patient_key) as unique_patients,
        sum(total_claim_cost) as total_claim_cost,
        sum(payer_coverage) as total_payer_coverage,
        sum(uncovered_amount) as total_uncovered_amount,
        avg(total_claim_cost) as avg_encounter_claim_cost,
        avg(encounter_duration_minutes) as avg_encounter_duration_minutes
    from {{ ref('fct_encounter') }}
    group by provider_key
),

procedure_agg as (
    select
        fe.provider_key,
        count(fp.procedure_fact_key) as total_procedures,
        sum(fp.base_cost) as total_procedure_cost
    from {{ ref('fct_procedure') }} fp
    join {{ ref('fct_encounter') }} fe
      on fp.encounter_key = fe.encounter_key
    group by fe.provider_key
),

medication_agg as (
    select
        fe.provider_key,
        count(fm.medication_fact_key) as total_medications,
        sum(fm.total_cost) as total_medication_cost
    from {{ ref('fct_medication') }} fm
    join {{ ref('fct_encounter') }} fe
      on fm.encounter_key = fe.encounter_key
    group by fe.provider_key
),

condition_agg as (
    select
        fe.provider_key,
        count(fc.condition_fact_key) as total_condition_events
    from {{ ref('fct_condition') }} fc
    join {{ ref('fct_encounter') }} fe
      on fc.encounter_key = fe.encounter_key
    group by fe.provider_key
)

select
    p.provider_key,
    p.provider_id,
    p.provider_name,
    p.gender,
    p.speciality,
    p.city,
    p.state,
    p.organization_id,

    o.organization_key,
    o.organization_name,

    coalesce(e.total_encounters, 0) as total_encounters,
    coalesce(e.unique_patients, 0) as unique_patients,
    coalesce(e.total_claim_cost, 0) as total_claim_cost,
    coalesce(e.total_payer_coverage, 0) as total_payer_coverage,
    coalesce(e.total_uncovered_amount, 0) as total_uncovered_amount,
    coalesce(e.avg_encounter_claim_cost, 0) as avg_encounter_claim_cost,
    coalesce(e.avg_encounter_duration_minutes, 0) as avg_encounter_duration_minutes,

    coalesce(pr.total_procedures, 0) as total_procedures,
    coalesce(pr.total_procedure_cost, 0) as total_procedure_cost,
    coalesce(m.total_medications, 0) as total_medications,
    coalesce(m.total_medication_cost, 0) as total_medication_cost,
    coalesce(c.total_condition_events, 0) as total_condition_events

from {{ ref('dim_provider') }} p
left join {{ ref('dim_organization') }} o
    on p.organization_id = o.organization_id
left join encounter_agg e
    on p.provider_key = e.provider_key
left join procedure_agg pr
    on p.provider_key = pr.provider_key
left join medication_agg m
    on p.provider_key = m.provider_key
left join condition_agg c
    on p.provider_key = c.provider_key