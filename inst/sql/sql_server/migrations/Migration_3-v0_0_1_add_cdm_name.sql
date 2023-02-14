{DEFAULT @table_prefix = ''}
{DEFAULT @cdm_source_info = cdm_source_info}
ALTER TABLE @database_schema.@table_prefix@cdm_source_info ADD COLUMN cdm_source_name VARCHAR;
UPDATE @database_schema.@table_prefix@cdm_source_info SET cdm_source_name = cdm_source_abbreviation;