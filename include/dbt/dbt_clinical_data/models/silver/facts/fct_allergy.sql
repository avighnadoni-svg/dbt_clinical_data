{{ config(materialized='table') }}

select
    PATIENT as patient_id,
    ENCOUNTER as encounter_id,
    CODE as allergy_code,
    DESCRIPTION as allergy_description,
    "START" as allergy_start_ts,
    "STOP" as allergy_stop_ts,
    case when "STOP" is null then 1 else 0 end as is_active,
    LOAD_TS,
    SOURCE_FILE,
    BATCH_ID
from {{ source('bronze', 'br_allergies') }}