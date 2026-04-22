{{ config(materialized='table') }}

select
    PATIENT as patient_id,
    ENCOUNTER as encounter_id,
    CODE as device_code,
    DESCRIPTION as device_description,
    UDI as udi,
    "START" as device_start_ts,
    "STOP" as device_stop_ts,
    datediff('day', "START", "STOP") as device_duration_days,
    case when "STOP" is null then 1 else 0 end as is_active,
    LOAD_TS,
    SOURCE_FILE,
    BATCH_ID
from {{ source('bronze', 'br_devices') }}