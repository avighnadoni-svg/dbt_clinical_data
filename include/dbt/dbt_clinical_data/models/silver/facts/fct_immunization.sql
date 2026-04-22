{{ config(materialized='table') }}

select
    PATIENT as patient_id,
    ENCOUNTER as encounter_id,
    CODE as immunization_code,
    DESCRIPTION as immunization_description,
    DATE as immunization_ts,
    BASE_COST as base_cost,
    LOAD_TS,
    SOURCE_FILE,
    BATCH_ID
from {{ source('bronze', 'br_immunizations') }}