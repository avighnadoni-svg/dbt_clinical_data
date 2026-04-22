{{ config(materialized='table') }}

select
    ID as careplan_id,
    PATIENT as patient_id,
    ENCOUNTER as encounter_id,
    CODE as careplan_code,
    DESCRIPTION as careplan_description,
    "START" as careplan_start_ts,
    "STOP" as careplan_stop_ts,
    REASONCODE as reason_code,
    REASONDESCRIPTION as reason_description,
    datediff('day', "START", "STOP") as careplan_duration_days,
    case when "STOP" is null then 1 else 0 end as is_active,
    LOAD_TS,
    SOURCE_FILE,
    BATCH_ID
from {{ source('bronze', 'br_careplans') }}