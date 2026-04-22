{{ config(materialized='table') }}

select
    ID as imaging_study_id,
    PATIENT as patient_id,
    ENCOUNTER as encounter_id,
    DATE as imaging_ts,
    BODYSITE_CODE as body_site_code,
    BODYSITE_DESCRIPTION as body_site_description,
    MODALITY_CODE as modality_code,
    MODALITY_DESCRIPTION as modality_description,
    SOP_CODE as sop_code,
    SOP_DESCRIPTION as sop_description,
    LOAD_TS,
    SOURCE_FILE,
    BATCH_ID
from {{ source('bronze', 'br_imaging_studies') }}