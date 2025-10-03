-- NOTE It is unlikely that these functions ever execute. This is the sql server/tsql code to rename tables
-- Postgresql, duckdb and sqlite are the supported results platforms and all these use their own syntax
{DEFAULT @covariate_mean = covariate_mean}
{DEFAULT @cse_covariate_mean = cse_covariate_mean}
{DEFAULT @covariate_definition = covariate_definition}
{DEFAULT @cse_covariate_definition = cse_covariate_definition}
{DEFAULT @cosine_similarity_score = cosine_similarity_score}
{DEFAULT @cse_cosine_similarity_score = cse_cosine_similarity_score}
{DEFAULT @cohort_count = cohort_count}
{DEFAULT @cse_cohort_count = cse_cohort_count}
{DEFAULT @cdm_source_info = cdm_source_info}
{DEFAULT @cse_cdm_source_info = cse_cdm_source_info}

ALTER TABLE @database_schema.@table_prefix@covariate_definition RENAME TO  @table_prefix@cse_covariate_definition;
ALTER TABLE @database_schema.@table_prefix@covariate_mean RENAME TO  @table_prefix@cse_covariate_mean;
ALTER TABLE @database_schema.@table_prefix@cosine_similarity_score RENAME TO @table_prefix@cse_cosine_similarity_score;
ALTER TABLE @database_schema.@table_prefix@cohort_count RENAME TO @table_prefix@cse_cohort_count;
ALTER TABLE @database_schema.@table_prefix@cdm_source_info RENAME TO @table_prefix@cse_cdm_source_info;