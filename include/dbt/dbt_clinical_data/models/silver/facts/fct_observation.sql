{{ config(materialized='table') }}

select
    DATE as observation_ts,
    PATIENT as patient_id,
    ENCOUNTER as encounter_id,
    CODE as observation_code,
    DESCRIPTION as observation_description,
    VALUE as observation_value_raw,
    try_to_number(VALUE) as observation_value_num,
    UNITS as units,
    TYPE as observation_type,
    LOAD_TS,
    SOURCE_FILE,
    BATCH_ID
from {{ source('bronze', 'br_observations') }}