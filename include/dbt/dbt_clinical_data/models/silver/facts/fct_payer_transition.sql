{{ config(materialized='table') }}

select
    PATIENT as patient_id,
    PAYER as payer_id,
    OWNERSHIP as ownership,
    START_YEAR as start_year,
    END_YEAR as end_year,
    (coalesce(END_YEAR, year(current_date)) - START_YEAR + 1) as coverage_years,
    LOAD_TS,
    SOURCE_FILE,
    BATCH_ID
from {{ source('bronze', 'br_payer_transitions') }}