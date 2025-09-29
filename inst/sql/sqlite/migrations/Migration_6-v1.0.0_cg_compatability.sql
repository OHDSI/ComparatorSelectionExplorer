{DEFAULT @table_prefix = ''}
{DEFAULT @atc_level = atc_level}
{DEFAULT @cohort_definition = cohort_definition}
{DEFAULT @cse_cohort_tag = cse_cohort_tag}

-- Drop table if exists
DROP TABLE IF EXISTS @database_schema.@table_prefixsubset_definition;

-- Add columns (SQLite does not support multiple ADD COLUMN in one statement)
ALTER TABLE @database_schema.@table_prefix@atc_level ADD COLUMN drug_concept_id_1 INTEGER;
ALTER TABLE @database_schema.@table_prefix@atc_level ADD COLUMN drug_concept_id_2 INTEGER;

-- UPDATE with JOIN workaround: use subqueries in SQLite
UPDATE @database_schema.@table_prefix@atc_level
SET drug_concept_id_1 = cohort_definition_id_1 / 1000
WHERE cohort_definition_id_1 IN (
    SELECT cohort_definition_id
    FROM @database_schema.@table_prefix@cohort_definition
    WHERE cohort_definition_id = @database_schema.@table_prefix@atc_level.cohort_definition_id_1
      AND atc_flag = 0
  );

UPDATE @database_schema.@table_prefix@atc_level
SET drug_concept_id_1 = cohort_definition_id_2 / 1000
WHERE cohort_definition_id_2 IN (
    SELECT cohort_definition_id
    FROM @database_schema.@table_prefix@cohort_definition
    WHERE cohort_definition_id = @database_schema.@table_prefix@atc_level.cohort_definition_id_2
      AND atc_flag = 0
  );

-- Create table
-- NOTE: DBI makes column names case sensetive, apparently?
CREATE TABLE @database_schema.@table_prefix@cse_cohort_tag (
   tag TEXT,
   cohort_definition_id BIGINT,
   PRIMARY KEY (TAG, COHORT_DEFINITION_ID)
);

-- Insert ATC tags
INSERT INTO @database_schema.@table_prefix@cse_cohort_tag (TAG, COHORT_DEFINITION_ID)
SELECT
    'ATC' AS TAG,
    COHORT_DEFINITION_ID
FROM @database_schema.@table_prefix@cohort_definition cd
WHERE cd.ATC_FLAG = 1;

-- Insert RxNorm tags
INSERT INTO @database_schema.@table_prefix@cse_cohort_tag (TAG, COHORT_DEFINITION_ID)
SELECT
    'RxNorm' AS TAG,
    COHORT_DEFINITION_ID
FROM @database_schema.@table_prefix@cohort_definition cd
WHERE cd.ATC_FLAG = 0;

-- Drop table
DROP TABLE IF EXISTS @database_schema.@table_prefix@cohort_definition;
