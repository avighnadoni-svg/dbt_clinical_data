{{ config(materialized='table') }}

with src as (
    select *
    from {{ source('bronze', 'br_encounters') }}
),

deduped as (
    select *
    from src
    qualify row_number() over (
        partition by coalesce(ID, concat(
            coalesce(PATIENT, ''), '|',
            coalesce(to_varchar("START"), ''), '|',
            coalesce(to_varchar("STOP"), ''), '|',
            coalesce(CODE, ''), '|',
            coalesce(ENCOUNTERCLASS, '')
        ))
        order by LOAD_TS desc, SOURCE_FILE desc, BATCH_ID desc
    ) = 1
),

final as (
    select
        md5(
            coalesce(
                d.ID,
                concat(
                    coalesce(d.PATIENT, ''), '|',
                    coalesce(to_varchar(d."START"), ''), '|',
                    coalesce(to_varchar(d."STOP"), ''), '|',
                    coalesce(d.CODE, ''), '|',
                    coalesce(d.ENCOUNTERCLASS, '')
                )
            )
        ) as encounter_key,

        d.ID as encounter_id,
        p.patient_key,
        pr.provider_key,
        o.organization_key,
        py.payer_key,
        ds.date_key as encounter_start_date_key,
        de.date_key as encounter_stop_date_key,
        cc.clinical_code_key as encounter_code_key,
        rc.clinical_code_key as reason_code_key,
        d.ENCOUNTERCLASS as encounter_class,
        d."START" as encounter_start_ts,
        d."STOP" as encounter_stop_ts,
        1 as encounter_count,
        d.BASE_ENCOUNTER_COST as base_encounter_cost,
        d.TOTAL_CLAIM_COST as total_claim_cost,
        d.PAYER_COVERAGE as payer_coverage,
        (d.TOTAL_CLAIM_COST - coalesce(d.PAYER_COVERAGE, 0)) as uncovered_amount,
        datediff('minute', d."START", d."STOP") as encounter_duration_minutes,
        d.LOAD_TS,
        d.SOURCE_FILE,
        d.BATCH_ID
    from deduped d
    left join {{ ref('dim_patient') }} p
        on d.PATIENT = p.patient_id
    left join {{ ref('dim_provider') }} pr
        on d.PROVIDER = pr.provider_id
    left join {{ ref('dim_organization') }} o
        on d.ORGANIZATION = o.organization_id
    left join {{ ref('dim_payer') }} py
        on d.PAYER = py.payer_id
    left join {{ ref('dim_date') }} ds
        on cast(d."START" as date) = ds.full_date
    left join {{ ref('dim_date') }} de
        on cast(d."STOP" as date) = de.full_date
    left join {{ ref('dim_clinical_code') }} cc
        on d.CODE = cc.code
       and cc.code_type = 'encounter'
    left join {{ ref('dim_clinical_code') }} rc
        on d.REASONCODE = rc.code
       and rc.code_type = 'condition'
)

select * from final