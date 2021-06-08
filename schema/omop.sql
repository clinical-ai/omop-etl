CREATE SCHEMA omop;
CREATE SCHEMA mapping;
SET search_path TO omop;


CREATE TABLE concept (
    concept_id INTEGER NULL,
    concept_name VARCHAR(255) NULL,
    domain_id VARCHAR(20) NULL,
    vocabulary_id VARCHAR(20) NULL,
    concept_class_id VARCHAR(20) NULL,
    standard_concept VARCHAR(1) NULL,
    concept_code VARCHAR(50) NULL,
    valid_start_date DATE NULL,
    valid_end_date DATE NULL,
    invalid_reason VARCHAR(1) NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE vocabulary (
    vocabulary_id VARCHAR(20) NULL,
    vocabulary_name VARCHAR(255) NULL,
    vocabulary_reference VARCHAR(255) NULL,
    vocabulary_version VARCHAR(255) NULL,
    vocabulary_concept_id INTEGER NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE domain (
    domain_id VARCHAR(20) NULL,
    domain_name VARCHAR(255) NULL,
    domain_concept_id INTEGER NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_class (
    concept_class_id VARCHAR(20) NULL,
    concept_class_name VARCHAR(255) NULL,
    concept_class_concept_id INTEGER NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_relationship (
    concept_id_1 INTEGER NULL,
    concept_id_2 INTEGER NULL,
    relationship_id VARCHAR(20) NULL,
    valid_start_date DATE NULL,
    valid_end_date DATE NULL,
    invalid_reason VARCHAR(1) NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE relationship (
    relationship_id VARCHAR(20) NULL,
    relationship_name VARCHAR(255) NULL,
    is_hierarchical VARCHAR(1) NULL,
    defines_ancestry VARCHAR(1) NULL,
    reverse_relationship_id VARCHAR(20) NULL,
    relationship_concept_id INTEGER NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_synonym (
    concept_id INTEGER NULL,
    concept_synonym_name VARCHAR(1000) NULL,
    language_concept_id INTEGER NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_ancestor (
    ancestor_concept_id INTEGER NULL,
    descendant_concept_id INTEGER NULL,
    min_levels_of_separation INTEGER NULL,
    max_levels_of_separation INTEGER NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE source_to_concept_map (
    source_code VARCHAR(50) NULL,
    source_concept_id INTEGER NULL,
    source_vocabulary_id VARCHAR(20) NULL,
    source_code_description VARCHAR(255) NULL,
    target_concept_id INTEGER NULL,
    target_vocabulary_id VARCHAR(20) NULL,
    valid_start_date DATE NULL,
    valid_end_date DATE NULL,
    invalid_reason VARCHAR(1) NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE drug_strength (
    drug_concept_id INTEGER NULL,
    ingredient_concept_id INTEGER NULL,
    amount_value NUMERIC NULL,
    amount_unit_concept_id INTEGER NULL,
    numerator_value NUMERIC NULL,
    numerator_unit_concept_id INTEGER NULL,
    denominator_value NUMERIC NULL,
    denominator_unit_concept_id INTEGER NULL,
    box_size INTEGER NULL,
    valid_start_date DATE NULL,
    valid_end_date DATE NULL,
    invalid_reason VARCHAR(1) NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE attribute_definition (
    attribute_definition_id INTEGER NULL,
    attribute_name VARCHAR(255) NULL,
    attribute_description TEXT NULL,
    attribute_type_concept_id INTEGER NULL,
    attribute_syntax TEXT NULL
);
/**************************
 
 Standardized meta-data
 
 ***************************/
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE cdm_source (
    cdm_source_name VARCHAR(255) NULL,
    cdm_source_abbreviation VARCHAR(25) NULL,
    cdm_holder VARCHAR(255) NULL,
    source_description TEXT NULL,
    source_documentation_reference VARCHAR(255) NULL,
    cdm_etl_reference VARCHAR(255) NULL,
    source_release_date DATE NULL,
    cdm_release_date DATE NULL,
    cdm_version VARCHAR(10) NULL,
    vocabulary_version VARCHAR(20) NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE metadata (
    metadata_concept_id INTEGER NULL,
    metadata_type_concept_id INTEGER NULL,
    name VARCHAR(250) NULL,
    value_as_string TEXT NULL,
    value_as_concept_id INTEGER NULL,
    metadata_date DATE NULL,
    metadata_datetime TIMESTAMP NULL
);
--INSERT INTO metadata (metadata_concept_id, metadata_type_concept_id, name, value_as_string, value_as_concept_id, metadata_date, metadata_datetime)
--VALUES (0, 0, 'CDM Version', '6.0', 0, NULL, NULL)
--;
/************************
 
 Standardized clinical data
 
 ************************/
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE person (
    person_id BIGINT NULL,
    gender_concept_id INTEGER NULL,
    year_of_birth INTEGER NULL,
    month_of_birth INTEGER NULL,
    day_of_birth INTEGER NULL,
    birth_datetime TIMESTAMP NULL,
    death_datetime TIMESTAMP NULL,
    race_concept_id INTEGER NULL,
    ethnicity_concept_id INTEGER NULL,
    location_id BIGINT NULL,
    provider_id BIGINT NULL,
    care_site_id BIGINT NULL,
    person_source_value VARCHAR(50) NULL,
    gender_source_value VARCHAR(50) NULL,
    gender_source_concept_id INTEGER NULL,
    race_source_value VARCHAR(50) NULL,
    race_source_concept_id INTEGER NULL,
    ethnicity_source_value VARCHAR(50) NULL,
    ethnicity_source_concept_id INTEGER NULL
);
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE observation_period (
    observation_period_id BIGINT NULL,
    person_id BIGINT NULL,
    observation_period_start_date DATE NULL,
    observation_period_end_date DATE NULL,
    period_type_concept_id INTEGER NULL
);
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE specimen (
    specimen_id BIGINT NULL,
    person_id BIGINT NULL,
    specimen_concept_id INTEGER NULL,
    specimen_type_concept_id INTEGER NULL,
    specimen_date DATE NULL,
    specimen_datetime TIMESTAMP NULL,
    quantity NUMERIC NULL,
    unit_concept_id INTEGER NULL,
    anatomic_site_concept_id INTEGER NULL,
    disease_status_concept_id INTEGER NULL,
    specimen_source_id VARCHAR(50) NULL,
    specimen_source_value VARCHAR(50) NULL,
    unit_source_value VARCHAR(50) NULL,
    anatomic_site_source_value VARCHAR(50) NULL,
    disease_status_source_value VARCHAR(50) NULL
);
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE visit_occurrence (
    visit_occurrence_id BIGINT NULL,
    person_id BIGINT NULL,
    visit_concept_id INTEGER NULL,
    visit_start_date DATE NULL,
    visit_start_datetime TIMESTAMP NULL,
    visit_end_date DATE NULL,
    visit_end_datetime TIMESTAMP NULL,
    visit_type_concept_id INTEGER NULL,
    provider_id BIGINT NULL,
    care_site_id BIGINT NULL,
    visit_source_value VARCHAR(50) NULL,
    visit_source_concept_id INTEGER NULL,
    admitted_from_concept_id INTEGER NULL,
    admitted_from_source_value VARCHAR(50) NULL,
    discharge_to_source_value VARCHAR(50) NULL,
    discharge_to_concept_id INTEGER NULL,
    preceding_visit_occurrence_id BIGINT NULL
);
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE visit_detail (
    visit_detail_id BIGINT NULL,
    person_id BIGINT NULL,
    visit_detail_concept_id INTEGER NULL,
    visit_detail_start_date DATE NULL,
    visit_detail_start_datetime TIMESTAMP NULL,
    visit_detail_end_date DATE NULL,
    visit_detail_end_datetime TIMESTAMP NULL,
    visit_detail_type_concept_id INTEGER NULL,
    provider_id BIGINT NULL,
    care_site_id BIGINT NULL,
    discharge_to_concept_id INTEGER NULL,
    admitted_from_concept_id INTEGER NULL,
    admitted_from_source_value VARCHAR(50) NULL,
    visit_detail_source_value VARCHAR(50) NULL,
    visit_detail_source_concept_id INTEGER NULL,
    discharge_to_source_value VARCHAR(50) NULL,
    preceding_visit_detail_id BIGINT NULL,
    visit_detail_parent_id BIGINT NULL,
    visit_occurrence_id BIGINT NULL
);
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE procedure_occurrence (
    procedure_occurrence_id BIGINT NULL,
    person_id BIGINT NULL,
    procedure_concept_id INTEGER NULL,
    procedure_date DATE NULL,
    procedure_datetime TIMESTAMP NULL,
    procedure_type_concept_id INTEGER NULL,
    modifier_concept_id INTEGER NULL,
    quantity INTEGER NULL,
    provider_id BIGINT NULL,
    visit_occurrence_id BIGINT NULL,
    visit_detail_id BIGINT NULL,
    procedure_source_value VARCHAR(50) NULL,
    procedure_source_concept_id INTEGER NULL,
    modifier_source_value VARCHAR(50) NULL
);
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE drug_exposure (
    drug_exposure_id BIGINT NULL,
    person_id BIGINT NULL,
    drug_concept_id INTEGER NULL,
    drug_exposure_start_date DATE NULL,
    drug_exposure_start_datetime TIMESTAMP NULL,
    drug_exposure_end_date DATE NULL,
    drug_exposure_end_datetime TIMESTAMP NULL,
    verbatim_end_date DATE NULL,
    drug_type_concept_id INTEGER NULL,
    stop_reason VARCHAR(20) NULL,
    refills INTEGER NULL,
    quantity NUMERIC NULL,
    days_supply INTEGER NULL,
    sig TEXT NULL,
    route_concept_id INTEGER NULL,
    lot_number VARCHAR(50) NULL,
    provider_id BIGINT NULL,
    visit_occurrence_id BIGINT NULL,
    visit_detail_id BIGINT NULL,
    drug_source_value VARCHAR(50) NULL,
    drug_source_concept_id INTEGER NULL,
    route_source_value VARCHAR(50) NULL,
    dose_unit_source_value VARCHAR(50) NULL
);
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE device_exposure (
    device_exposure_id BIGINT NULL,
    person_id BIGINT NULL,
    device_concept_id INTEGER NULL,
    device_exposure_start_date DATE NULL,
    device_exposure_start_datetime TIMESTAMP NULL,
    device_exposure_end_date DATE NULL,
    device_exposure_end_datetime TIMESTAMP NULL,
    device_type_concept_id INTEGER NULL,
    unique_device_id VARCHAR(50) NULL,
    quantity INTEGER NULL,
    provider_id BIGINT NULL,
    visit_occurrence_id BIGINT NULL,
    visit_detail_id BIGINT NULL,
    device_source_value VARCHAR(100) NULL,
    device_source_concept_id INTEGER NULL
);
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE condition_occurrence (
    condition_occurrence_id BIGINT NULL,
    person_id BIGINT NULL,
    condition_concept_id INTEGER NULL,
    condition_start_date DATE NULL,
    condition_start_datetime TIMESTAMP NULL,
    condition_end_date DATE NULL,
    condition_end_datetime TIMESTAMP NULL,
    condition_type_concept_id INTEGER NULL,
    condition_status_concept_id INTEGER NULL,
    stop_reason VARCHAR(20) NULL,
    provider_id BIGINT NULL,
    visit_occurrence_id BIGINT NULL,
    visit_detail_id BIGINT NULL,
    condition_source_value VARCHAR(50) NULL,
    condition_source_concept_id INTEGER NULL,
    condition_status_source_value VARCHAR(50) NULL
);
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE measurement (
    measurement_id BIGINT NULL,
    person_id BIGINT NULL,
    measurement_concept_id INTEGER NULL,
    measurement_date DATE NULL,
    measurement_datetime TIMESTAMP NULL,
    measurement_time VARCHAR(10) NULL,
    measurement_type_concept_id INTEGER NULL,
    operator_concept_id INTEGER NULL,
    value_as_number NUMERIC NULL,
    value_as_concept_id INTEGER NULL,
    unit_concept_id INTEGER NULL,
    range_low NUMERIC NULL,
    range_high NUMERIC NULL,
    provider_id BIGINT NULL,
    visit_occurrence_id BIGINT NULL,
    visit_detail_id BIGINT NULL,
    measurement_source_value VARCHAR(50) NULL,
    measurement_source_concept_id INTEGER NULL,
    unit_source_value VARCHAR(50) NULL,
    value_source_value VARCHAR(50) NULL
);
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE note (
    note_id BIGINT NULL,
    person_id BIGINT NULL,
    note_event_id BIGINT NULL,
    note_event_field_concept_id INTEGER NULL,
    note_date DATE NULL,
    note_datetime TIMESTAMP NULL,
    note_type_concept_id INTEGER NULL,
    note_class_concept_id INTEGER NULL,
    note_title VARCHAR(250) NULL,
    note_text TEXT NULL,
    encoding_concept_id INTEGER NULL,
    language_concept_id INTEGER NULL,
    provider_id BIGINT NULL,
    visit_occurrence_id BIGINT NULL,
    visit_detail_id BIGINT NULL,
    note_source_value VARCHAR(50) NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE note_nlp (
    note_nlp_id BIGINT NULL,
    note_id BIGINT NULL,
    section_concept_id INTEGER NULL,
    snippet VARCHAR(250) NULL,
    "offset" VARCHAR(250) NULL,
    lexical_variant VARCHAR(250) NULL,
    note_nlp_concept_id INTEGER NULL,
    nlp_system VARCHAR(250) NULL,
    nlp_date DATE NULL,
    nlp_datetime TIMESTAMP NULL,
    term_exists VARCHAR(1) NULL,
    term_temporal VARCHAR(50) NULL,
    term_modifiers VARCHAR(2000) NULL,
    note_nlp_source_concept_id INTEGER NULL
);
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE observation (
    observation_id BIGINT NULL,
    person_id BIGINT NULL,
    observation_concept_id INTEGER NULL,
    observation_date DATE NULL,
    observation_datetime TIMESTAMP NULL,
    observation_type_concept_id INTEGER NULL,
    value_as_number NUMERIC NULL,
    value_as_string VARCHAR(60) NULL,
    value_as_concept_id INTEGER NULL,
    qualifier_concept_id INTEGER NULL,
    unit_concept_id INTEGER NULL,
    provider_id BIGINT NULL,
    visit_occurrence_id BIGINT NULL,
    visit_detail_id BIGINT NULL,
    observation_source_value VARCHAR(50) NULL,
    observation_source_concept_id INTEGER NULL,
    unit_source_value VARCHAR(50) NULL,
    qualifier_source_value VARCHAR(50) NULL,
    observation_event_id BIGINT NULL,
    obs_event_field_concept_id INTEGER NULL,
    value_as_datetime TIMESTAMP NULL
);
--HINT DISTRIBUTE ON KEY(person_id)
CREATE TABLE survey_conduct (
    survey_conduct_id BIGINT NULL,
    person_id BIGINT NULL,
    survey_concept_id INTEGER NULL,
    survey_start_date DATE NULL,
    survey_start_datetime TIMESTAMP NULL,
    survey_end_date DATE NULL,
    survey_end_datetime TIMESTAMP NULL,
    provider_id BIGINT NULL,
    assisted_concept_id INTEGER NULL,
    respondent_type_concept_id INTEGER NULL,
    timing_concept_id INTEGER NULL,
    collection_method_concept_id INTEGER NULL,
    assisted_source_value VARCHAR(50) NULL,
    respondent_type_source_value VARCHAR(100) NULL,
    timing_source_value VARCHAR(100) NULL,
    collection_method_source_value VARCHAR(100) NULL,
    survey_source_value VARCHAR(100) NULL,
    survey_source_concept_id INTEGER NULL,
    survey_source_identifier VARCHAR(100) NULL,
    validated_survey_concept_id INTEGER NULL,
    validated_survey_source_value VARCHAR(100) NULL,
    survey_version_number VARCHAR(20) NULL,
    visit_occurrence_id BIGINT NULL,
    visit_detail_id BIGINT NULL,
    response_visit_occurrence_id BIGINT NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE fact_relationship (
    domain_concept_id_1 INTEGER NULL,
    fact_id_1 BIGINT NULL,
    domain_concept_id_2 INTEGER NULL,
    fact_id_2 BIGINT NULL,
    relationship_concept_id INTEGER NULL
);
/************************
 
 Standardized health system data
 
 ************************/
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE location(
    location_id BIGINT NULL,
    address_1 VARCHAR(50) NULL,
    address_2 VARCHAR(50) NULL,
    city VARCHAR(50) NULL,
    state VARCHAR(3) NULL,
    zip VARCHAR(9) NULL,
    county VARCHAR(20) NULL,
    country VARCHAR(100) NULL,
    location_source_value VARCHAR(50) NULL,
    latitude NUMERIC NULL,
    longitude NUMERIC NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE location_history --Table added
(
    location_history_id BIGINT NULL,
    location_id BIGINT NULL,
    relationship_type_concept_id INTEGER NULL,
    domain_id VARCHAR(50) NULL,
    entity_id BIGINT NULL,
    start_date DATE NULL,
    end_date DATE NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE care_site (
    care_site_id BIGINT NULL,
    care_site_name VARCHAR(255) NULL,
    place_of_service_concept_id INTEGER NULL,
    location_id BIGINT NULL,
    care_site_source_value VARCHAR(50) NULL,
    place_of_service_source_value VARCHAR(50) NULL
);
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE provider (
    provider_id BIGINT NULL,
    provider_name VARCHAR(255) NULL,
    NPI VARCHAR(20) NULL,
    DEA VARCHAR(20) NULL,
    specialty_concept_id INTEGER NULL,
    care_site_id BIGINT NULL,
    year_of_birth INTEGER NULL,
    gender_concept_id INTEGER NULL,
    provider_source_value VARCHAR(50) NULL,
    specialty_source_value VARCHAR(50) NULL,
    specialty_source_concept_id INTEGER NULL,
    gender_source_value VARCHAR(50) NULL,
    gender_source_concept_id INTEGER NULL
);
/************************
 
 Standardized health economics
 
 ************************/
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE payer_plan_period (
    payer_plan_period_id BIGINT NULL,
    person_id BIGINT NULL,
    contract_person_id BIGINT NULL,
    payer_plan_period_start_date DATE NULL,
    payer_plan_period_end_date DATE NULL,
    payer_concept_id INTEGER NULL,
    plan_concept_id INTEGER NULL,
    contract_concept_id INTEGER NULL,
    sponsor_concept_id INTEGER NULL,
    stop_reason_concept_id INTEGER NULL,
    payer_source_value VARCHAR(50) NULL,
    payer_source_concept_id INTEGER NULL,
    plan_source_value VARCHAR(50) NULL,
    plan_source_concept_id INTEGER NULL,
    contract_source_value VARCHAR(50) NULL,
    contract_source_concept_id INTEGER NULL,
    sponsor_source_value VARCHAR(50) NULL,
    sponsor_source_concept_id INTEGER NULL,
    family_source_value VARCHAR(50) NULL,
    stop_reason_source_value VARCHAR(50) NULL,
    stop_reason_source_concept_id INTEGER NULL
);
--HINT DISTRIBUTE ON KEY(person_id)
CREATE TABLE cost (
    cost_id BIGINT NULL,
    person_id BIGINT NULL,
    cost_event_id BIGINT NULL,
    cost_event_field_concept_id INTEGER NULL,
    cost_concept_id INTEGER NULL,
    cost_type_concept_id INTEGER NULL,
    currency_concept_id INTEGER NULL,
    cost NUMERIC NULL,
    incurred_date DATE NULL,
    billed_date DATE NULL,
    paid_date DATE NULL,
    revenue_code_concept_id INTEGER NULL,
    drg_concept_id INTEGER NULL,
    cost_source_value VARCHAR(50) NULL,
    cost_source_concept_id INTEGER NULL,
    revenue_code_source_value VARCHAR(50) NULL,
    drg_source_value VARCHAR(3) NULL,
    payer_plan_period_id BIGINT NULL
);
/************************
 
 Standardized derived elements
 
 ************************/
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE drug_era (
    drug_era_id BIGINT NULL,
    person_id BIGINT NULL,
    drug_concept_id INTEGER NULL,
    drug_era_start_datetime TIMESTAMP NULL,
    drug_era_end_datetime TIMESTAMP NULL,
    drug_exposure_count INTEGER NULL,
    gap_days INTEGER NULL
);
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE dose_era (
    dose_era_id BIGINT NULL,
    person_id BIGINT NULL,
    drug_concept_id INTEGER NULL,
    unit_concept_id INTEGER NULL,
    dose_value NUMERIC NULL,
    dose_era_start_datetime TIMESTAMP NULL,
    dose_era_end_datetime TIMESTAMP NULL
);
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE condition_era (
    condition_era_id BIGINT NULL,
    person_id BIGINT NULL,
    condition_concept_id INTEGER NULL,
    condition_era_start_datetime TIMESTAMP NULL,
    condition_era_end_datetime TIMESTAMP NULL,
    condition_occurrence_count INTEGER NULL
);