{{ config(materialized='table') }}

select
    ID as payer_id,
    NAME as payer_name,
    ADDRESS as address,
    CITY as city,
    STATE_HEADQUARTERED as state_headquartered,
    ZIP as zip,
    PHONE as phone,
    AMOUNT_COVERED as amount_covered,
    AMOUNT_UNCOVERED as amount_uncovered,
    REVENUE as revenue,
    COVERED_ENCOUNTERS as covered_encounters,
    UNCOVERED_ENCOUNTERS as uncovered_encounters,
    COVERED_MEDICATIONS as covered_medications,
    UNCOVERED_MEDICATIONS as uncovered_medications,
    COVERED_PROCEDURES as covered_procedures,
    UNCOVERED_PROCEDURES as uncovered_procedures,
    COVERED_IMMUNIZATIONS as covered_immunizations,
    UNCOVERED_IMMUNIZATIONS as uncovered_immunizations,
    UNIQUE_CUSTOMERS as unique_customers,
    QOLS_AVG as qols_avg,
    MEMBER_MONTHS as member_months,
    LOAD_TS,
    SOURCE_FILE,
    BATCH_ID
from {{ source('bronze', 'br_payers') }}