{{ config(materialized='table') }}

select
    PATIENT as patient_id,
    ENCOUNTER as encounter_id,
    CODE as supply_code,
    DESCRIPTION as supply_description,
    DATE as supply_ts,
    QUANTITY as quantity,
    LOAD_TS,
    SOURCE_FILE,
    BATCH_ID
from {{ source('bronze', 'br_supplies') }}