{DEFAULT @table_prefix = ''}
{DEFAULT @cohort_definition = cohort_definition}
{DEFAULT @cdm_source_info = cdm_source_info}

ALTER TABLE @database_schema.@table_prefix@cohort_definition ADD COLUMN subset_parent BIGINT;
UPDATE @database_schema.@table_prefix@cohort_definition SET subset_parent = cohort_definition_id;

ALTER TABLE @database_schema.@table_prefix@cdm_source_info ADD COLUMN cdm_version_concept_id INT;
UPDATE @database_schema.@table_prefix@cdm_source_info SET cdm_version_concept_id = 0;