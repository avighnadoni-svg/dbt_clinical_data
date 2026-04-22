{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['ID']) }} as organization_key,
    ID as organization_id,
    NAME as organization_name,
    ADDRESS as address,
    CITY as city,
    STATE as state,
    ZIP as zip,
    LAT as latitude,
    LON as longitude,
    PHONE as phone,
    REVENUE as revenue,
    UTILIZATION as utilization,
    LOAD_TS,
    SOURCE_FILE,
    BATCH_ID
from {{ source('bronze', 'br_organizations') }}