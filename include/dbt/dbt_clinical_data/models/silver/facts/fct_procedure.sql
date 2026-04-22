{{ config(materialized='table') }}

select
    PATIENT as patient_id,
    ENCOUNTER as encounter_id,
    CODE as procedure_code,
    DESCRIPTION as procedure_description,
    DATE as procedure_ts,
    BASE_COST as base_cost,
    REASONCODE as reason_code,
    REASONDESCRIPTION as reason_description,
    LOAD_TS,
    SOURCE_FILE,
    BATCH_ID
from {{ source('bronze', 'br_procedures') }}