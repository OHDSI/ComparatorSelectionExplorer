{DEFAULT @table_prefix = ''}
{DEFAULT @cohort_definition = cohort_definition}
{DEFAULT @cdm_source_info = cdm_source_info}
{DEFAULT @cohort_count = cohort_count}
{DEFAULT @cosine_similarity = cosine_similarity}
{DEFAULT @covariate_definition = covariate_definition}
{DEFAULT @covariate_mean = covariate_mean}
{DEFAULT @package_version = package_version}

CREATE TABLE @database_schema.@table_prefix@cohort_definition (
  	 cohort_definition_id BIGINT NOT NULL,
	 concept_id BIGINT,
	 cohort_definition_name VARCHAR,
	 short_name VARCHAR,
	 atc_flag INT,
	 PRIMARY KEY(cohort_definition_id)
);
 
CREATE TABLE @database_schema.@table_prefix@cdm_source_info (
  	 database_id BIGINT PRIMARY KEY NOT NULL,
	 cdm_source_abbreviation VARCHAR,
	 cdm_holder VARCHAR,
	 source_description VARCHAR,
	 source_documentation_reference VARCHAR,
	 cdm_etl_reference VARCHAR,
	 source_release_date DATE,
	 cdm_release_date DATE,
	 cdm_version VARCHAR,
	 vocabulary_version VARCHAR
);
 
CREATE TABLE @database_schema.@table_prefix@cohort_count (
  	 num_persons BIGINT,
	 cohort_definition_id BIGINT NOT NULL,
	 database_id BIGINT NOT NULL,
	 PRIMARY KEY(cohort_definition_id, database_id)
);
 
CREATE TABLE @database_schema.@table_prefix@cosine_similarity_score (
  	 database_id BIGINT NOT NULL,
	 cohort_definition_id_1 BIGINT NOT NULL,
	 cohort_definition_id_2 BIGINT NOT NULL,
	 covariate_type VARCHAR(25),
	 cosine_similarity FLOAT,
	 PRIMARY KEY(cohort_definition_id_1, cohort_definition_id_2, database_id, covariate_type)
);
 
CREATE TABLE @database_schema.@table_prefix@covariate_definition (
  	 covariate_id BIGINT PRIMARY KEY,
	 covariate_name VARCHAR,
	 concept_id BIGINT,
	 time_at_risk_start INT,
	 time_at_risk_end INT,
	 covariate_type VARCHAR
);
 
CREATE TABLE @database_schema.@table_prefix@covariate_mean (
  	 database_id BIGINT NOT NULL,
	 cohort_definition_id BIGINT  NOT NULL,
	 covariate_id BIGINT  NOT NULL,
	 covariate_mean FLOAT NOT NULL,
	 PRIMARY KEY(cohort_definition_id, database_id, covariate_id)
);

CREATE TABLE @database_schema.@table_prefix@package_version (
    version_number VARCHAR NOT NULL PRIMARY KEY
);