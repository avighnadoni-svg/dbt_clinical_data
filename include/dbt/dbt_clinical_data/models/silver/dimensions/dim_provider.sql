{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['ID']) }} as provider_key,
    ID as provider_id,
    ORGANIZATION as organization_id,
    NAME as provider_name,
    GENDER as gender,
    SPECIALITY as speciality,
    ADDRESS as address,
    CITY as city,
    STATE as state,
    ZIP as zip,
    LAT as latitude,
    LON as longitude,
    UTILIZATION as utilization,
    LOAD_TS,
    SOURCE_FILE,
    BATCH_ID
from {{ source('bronze', 'br_providers') }}