{{ config(materialized='table') }}

select
    ID as encounter_id,
    "START" as encounter_start_ts,
    "STOP" as encounter_stop_ts,
    PATIENT as patient_id,
    ORGANIZATION as organization_id,
    PROVIDER as provider_id,
    PAYER as payer_id,
    ENCOUNTERCLASS as encounter_class,
    CODE as encounter_code,
    DESCRIPTION as encounter_description,
    BASE_ENCOUNTER_COST as base_encounter_cost,
    TOTAL_CLAIM_COST as total_claim_cost,
    PAYER_COVERAGE as payer_coverage,
    (TOTAL_CLAIM_COST - coalesce(PAYER_COVERAGE,0)) as uncovered_amount,
    REASONCODE as reason_code,
    REASONDESCRIPTION as reason_description,
    datediff('minute', "START", "STOP") as encounter_duration_minutes,
    LOAD_TS,
    SOURCE_FILE,
    BATCH_ID
from {{ source('bronze', 'br_encounters') }}