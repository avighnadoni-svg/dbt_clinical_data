{{ config(materialized='table') }}

with encounter_agg as (
    select
        patient_key,
        min(encounter_start_ts) as first_encounter_ts,
        max(encounter_stop_ts) as last_encounter_ts,
        count(*) as total_encounters,
        sum(total_claim_cost) as total_claim_cost,
        sum(payer_coverage) as total_payer_coverage,
        sum(uncovered_amount) as total_uncovered_amount,
        avg(encounter_duration_minutes) as avg_encounter_duration_minutes
    from {{ ref('fct_encounter') }}
    group by patient_key
),

condition_agg as (
    select
        patient_key,
        count(*) as total_conditions
    from {{ ref('fct_condition') }}
    group by patient_key
),

observation_agg as (
    select
        patient_key,
        count(*) as total_observations
    from {{ ref('fct_observation') }}
    group by patient_key
),

medication_agg as (
    select
        patient_key,
        count(*) as total_medications,
        sum(total_cost) as total_medication_cost
    from {{ ref('fct_medication') }}
    group by patient_key
),

procedure_agg as (
    select
        patient_key,
        count(*) as total_procedures,
        sum(base_cost) as total_procedure_cost
    from {{ ref('fct_procedure') }}
    group by patient_key
),

careplan_agg as (
    select
        patient_key,
        count(*) as total_careplans
    from {{ ref('fct_careplan') }}
    group by patient_key
),

device_agg as (
    select
        patient_key,
        count(*) as total_devices
    from {{ ref('fct_device') }}
    group by patient_key
),

immunization_agg as (
    select
        patient_key,
        count(*) as total_immunizations
    from {{ ref('fct_immunization') }}
    group by patient_key
),

imaging_agg as (
    select
        patient_key,
        count(*) as total_imaging_studies
    from {{ ref('fct_imaging_study') }}
    group by patient_key
),

allergy_agg as (
    select
        patient_key,
        count(*) as total_allergies
    from {{ ref('fct_allergy') }}
    group by patient_key
),

payer_transition_agg as (
    select
        patient_key,
        count(*) as total_payer_transitions,
        sum(coverage_years) as total_coverage_years
    from {{ ref('fct_payer_transition') }}
    group by patient_key
)

select
    p.patient_key,
    p.patient_id,
    p.first_name,
    p.last_name,
    p.gender,
    p.race,
    p.ethnicity,
    p.age,
    p.age_band,
    p.city,
    p.state,
    p.is_deceased,

    e.first_encounter_ts,
    e.last_encounter_ts,
    e.total_encounters,
    e.total_claim_cost,
    e.total_payer_coverage,
    e.total_uncovered_amount,
    e.avg_encounter_duration_minutes,

    coalesce(c.total_conditions, 0) as total_conditions,
    coalesce(o.total_observations, 0) as total_observations,
    coalesce(m.total_medications, 0) as total_medications,
    coalesce(m.total_medication_cost, 0) as total_medication_cost,
    coalesce(pr.total_procedures, 0) as total_procedures,
    coalesce(pr.total_procedure_cost, 0) as total_procedure_cost,
    coalesce(cp.total_careplans, 0) as total_careplans,
    coalesce(d.total_devices, 0) as total_devices,
    coalesce(i.total_immunizations, 0) as total_immunizations,
    coalesce(img.total_imaging_studies, 0) as total_imaging_studies,
    coalesce(a.total_allergies, 0) as total_allergies,
    coalesce(pt.total_payer_transitions, 0) as total_payer_transitions,
    coalesce(pt.total_coverage_years, 0) as total_coverage_years

from {{ ref('dim_patient') }} p
left join encounter_agg e on p.patient_key = e.patient_key
left join condition_agg c on p.patient_key = c.patient_key
left join observation_agg o on p.patient_key = o.patient_key
left join medication_agg m on p.patient_key = m.patient_key
left join procedure_agg pr on p.patient_key = pr.patient_key
left join careplan_agg cp on p.patient_key = cp.patient_key
left join device_agg d on p.patient_key = d.patient_key
left join immunization_agg i on p.patient_key = i.patient_key
left join imaging_agg img on p.patient_key = img.patient_key
left join allergy_agg a on p.patient_key = a.patient_key
left join payer_transition_agg pt on p.patient_key = pt.patient_key