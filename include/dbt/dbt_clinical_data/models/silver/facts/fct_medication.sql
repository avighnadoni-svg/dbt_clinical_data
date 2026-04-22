{{ config(materialized='table') }}

select
    PATIENT as patient_id,
    PAYER as payer_id,
    ENCOUNTER as encounter_id,
    CODE as medication_code,
    DESCRIPTION as medication_description,
    "START" as medication_start_ts,
    "STOP" as medication_stop_ts,
    BASE_COST as base_cost,
    PAYER_COVERAGE as payer_coverage,
    DISPENSES as dispenses,
    TOTALCOST as total_cost,
    REASONCODE as reason_code,
    REASONDESCRIPTION as reason_description,
    datediff('day', "START", "STOP") as medication_duration_days,
    LOAD_TS,
    SOURCE_FILE,
    BATCH_ID
from {{ source('bronze', 'br_medications') }}