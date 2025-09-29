{DEFAULT @table_prefix = ''}
{DEFAULT @atc_level = atc_level}
{DEFAULT @cohort_definition = cohort_definition}
{DEFAULT @cse_cohort_tag = cse_cohort_tag}

DROP TABLE IF EXISTS @database_schema.@table_prefixsubset_definition;


ALTER TABLE @database_schema.@table_prefix@atc_level ADD column drug_concept_id_1 BIGINT;
ALTER TABLE @database_schema.@table_prefix@atc_level ADD column drug_concept_id_2 BIGINT;

UPDATE a SET drug_concept_id_1 = cohort_definition_id_1/1000
FROM  @database_schema.@table_prefix@atc_level a
INNER JOIN @database_schema.@table_prefix@cohort_definition cd ON cd.cohort_definition_id = a.cohort_definition_id_1 AND cd.atc_flag = 0;

UPDATE a SET drug_concept_id_1 = cohort_definition_id_2/1000
FROM  @database_schema.@table_prefix@atc_level a
INNER JOIN @database_schema.@table_prefix@cohort_definition cd ON cd.cohort_definition_id = a.cohort_definition_id_2 AND cd.atc_flag = 0;

CREATE TABLE @database_schema.@table_prefix@cse_cohort_tag (
   TAG VARCHAR,
   COHORT_DEFINITION_ID BIGINT,
   PRIMARY KEY (TAG, COHORT_DEFINITION_ID)
);

INSERT INTO @database_schema.@table_prefix@cse_cohort_tag (TAG, COHORT_DEFINITION_ID)
SELECT
    'ATC' as TAG,
    COHORT_DEFINITION_ID
FROM @database_schema.@table_prefix@cohort_definition cd
WHERE cd.ATC_FLG = 1;

INSERT INTO @database_schema.@table_prefix@cse_cohort_tag (TAG, COHORT_DEFINITION_ID)
SELECT
    'RxNorm' as TAG,
    COHORT_DEFINITION_ID
FROM @database_schema.@table_prefix@cohort_definition cd
WHERE cd.ATC_FLG = 0;


DROP TABLE IF EXISTS @database_schema.@table_prefix@cohort_definition;
