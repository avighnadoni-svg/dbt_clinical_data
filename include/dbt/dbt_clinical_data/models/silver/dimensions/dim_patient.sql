{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['ID']) }} as patient_key,
    ID as patient_id,
    BIRTHDATE as birth_date,
    DEATHDATE as death_date,
    FIRST as first_name,
    LAST as last_name,
    MARITAL as marital_status,
    RACE as race,
    ETHNICITY as ethnicity,
    GENDER as gender,
    BIRTHPLACE as birth_place,
    ADDRESS as address,
    CITY as city,
    STATE as state,
    COUNTY as county,
    ZIP as zip,
    LAT as latitude,
    LON as longitude,
    HEALTHCARE_EXPENSES as healthcare_expenses,
    HEALTHCARE_COVERAGE as healthcare_coverage,
    case when DEATHDATE is not null then 1 else 0 end as is_deceased,
    datediff('year', BIRTHDATE, current_date) as age,
    case
        when datediff('year', BIRTHDATE, current_date) < 18 then '0-17'
        when datediff('year', BIRTHDATE, current_date) between 18 and 35 then '18-35'
        when datediff('year', BIRTHDATE, current_date) between 36 and 50 then '36-50'
        when datediff('year', BIRTHDATE, current_date) between 51 and 65 then '51-65'
        else '66+'
    end as age_band,
    LOAD_TS,
    SOURCE_FILE,
    BATCH_ID
from {{ source('bronze', 'br_patients') }}