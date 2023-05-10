{DEFAULT @table_prefix = ''}
{DEFAULT @cohort_definition = cohort_definition}
ALTER TABLE @database_schema.@table_prefix@cohort_definition ADD COLUMN subset_parent BIGINT;
UPDATE @database_schema.@table_prefix@cohort_definition SET subset_parent = cohort_definition_id;