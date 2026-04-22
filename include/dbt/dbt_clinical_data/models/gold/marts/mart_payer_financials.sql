{{ config(materialized='table') }}

with encounter_agg as (
    select
        payer_key,
        count(*) as total_encounters,
        count(distinct patient_key) as unique_patients,
        sum(total_claim_cost) as total_claim_cost,
        sum(payer_coverage) as total_payer_coverage,
        sum(uncovered_amount) as total_uncovered_amount
    from {{ ref('fct_encounter') }}
    group by payer_key
),

medication_agg as (
    select
        payer_key,
        count(*) as total_medications,
        sum(total_cost) as total_medication_cost,
        sum(payer_coverage) as medication_payer_coverage
    from {{ ref('fct_medication') }}
    group by payer_key
),

payer_transition_agg as (
    select
        payer_key,
        count(*) as total_transitions,
        sum(coverage_years) as total_coverage_years
    from {{ ref('fct_payer_transition') }}
    group by payer_key
)

select
    p.payer_key,
    p.payer_id,
    p.payer_name,
    p.city,
    p.state_headquartered,
    p.phone,

    coalesce(e.total_encounters, 0) as total_encounters,
    coalesce(e.unique_patients, 0) as unique_patients,
    coalesce(e.total_claim_cost, 0) as total_claim_cost,
    coalesce(e.total_payer_coverage, 0) as total_payer_coverage,
    coalesce(e.total_uncovered_amount, 0) as total_uncovered_amount,

    coalesce(m.total_medications, 0) as total_medications,
    coalesce(m.total_medication_cost, 0) as total_medication_cost,
    coalesce(m.medication_payer_coverage, 0) as medication_payer_coverage,

    coalesce(pt.total_transitions, 0) as total_transitions,
    coalesce(pt.total_coverage_years, 0) as total_coverage_years,

    p.amount_covered as source_amount_covered,
    p.amount_uncovered as source_amount_uncovered,
    p.revenue as source_revenue,
    p.covered_encounters as source_covered_encounters,
    p.uncovered_encounters as source_uncovered_encounters,
    p.covered_medications as source_covered_medications,
    p.uncovered_medications as source_uncovered_medications,
    p.covered_procedures as source_covered_procedures,
    p.uncovered_procedures as source_uncovered_procedures,
    p.covered_immunizations as source_covered_immunizations,
    p.uncovered_immunizations as source_uncovered_immunizations,
    p.unique_customers as source_unique_customers,
    p.qols_avg,
    p.member_months

from {{ ref('dim_payer') }} p
left join encounter_agg e
    on p.payer_key = e.payer_key
left join medication_agg m
    on p.payer_key = m.payer_key
left join payer_transition_agg pt
    on p.payer_key = pt.payer_key