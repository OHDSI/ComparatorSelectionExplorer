{DEFAULT @table_prefix = ''}
{DEFAULT @cdm_source_info = cdm_source_info}
ALTER TABLE @database_schema.@table_prefix@cdm_source_info ADD COLUMN cdm_version_concept_id INT;
UPDATE @database_schema.@table_prefix@cdm_source_info SET cdm_version_concept_id = 0;