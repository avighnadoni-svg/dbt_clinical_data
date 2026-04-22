{{ config(materialized='table') }}

with src as (
    select * 
    from {{ source('bronze', 'br_payer_transitions') }}
),

start_dates as (
    select
        *,
        to_date(concat(START_YEAR, '-01-01')) as start_dt,
        to_date(concat(coalesce(END_YEAR, year(current_date())), '-12-31')) as end_dt
    from src
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            's.PATIENT',
            's.PAYER',
            's.START_YEAR',
            's.END_YEAR'
        ]) }} as payer_transition_fact_key,

        p.patient_key,
        py.payer_key,

        ds.date_key as start_year_date_key,
        de.date_key as end_year_date_key,

        s.OWNERSHIP as ownership,
        1 as transition_count,
        (coalesce(s.END_YEAR, year(current_date())) - s.START_YEAR + 1) as coverage_years,

        s.LOAD_TS,
        s.SOURCE_FILE,
        s.BATCH_ID
    from start_dates s
    left join {{ ref('dim_patient') }} p
        on s.PATIENT = p.patient_id
    left join {{ ref('dim_payer') }} py
        on s.PAYER = py.payer_id
    left join {{ ref('dim_date') }} ds
        on s.start_dt = ds.full_date
    left join {{ ref('dim_date') }} de
        on s.end_dt = de.full_date
)

select * from final