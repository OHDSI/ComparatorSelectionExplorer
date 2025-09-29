{DEFAULT @table_prefix = ''}
{DEFAULT @atc_level = atc_level}
{DEFAULT @cse_atc_level = cse_atc_level}
{DEFAULT @cohort_definition = cohort_definition}
{DEFAULT @cse_cohort_tag = cse_cohort_tag}

DROP TABLE IF EXISTS @database_schema.@table_prefixsubset_definition;

CREATE TABLE @database_schema.@table_prefix@cse_atc_level (
    drug_concept_id_1 bigint,
	drug_concept_id_2 bigint,
	level_closest_atc_relation int,
	level_furthest_atc_relation int,
	atc_1_related int,
	atc_2_related int,
	atc_3_related int,
	atc_4_related int,
	atc_5_related int,
	PRIMARY KEY (drug_concept_id_1, drug_concept_id_2)
);

INSERT INTO @database_schema.@table_prefix@cse_atc_level
            (drug_concept_id_1, drug_concept_id_2, level_closest_atc_relation, level_furthest_atc_relation, atc_1_related, atc_2_related, atc_3_related, atc_4_related, atc_5_related)

SELECT
    a.cohort_definition_id_1/1000 as drug_concept_id_1,
    a.cohort_definition_id_2/1000 as drug_concept_id_2,
    a.level_closest_atc_relation,
	a.level_furthest_atc_relation,
	a.atc_1_related,
	a.atc_2_related,
	a.atc_3_related,
	a.atc_4_related,
	a.atc_5_related
FROM  @database_schema.@table_prefix@atc_level a
INNER JOIN @database_schema.@table_prefix@cohort_definition cd ON cd.cohort_definition_id = a.cohort_definition_id_1 AND cd.atc_flag = 0;

CREATE TABLE @database_schema.@table_prefix@cse_cohort_tag (
   tag VARCHAR,
   cohort_definition_id BIGINT,
   PRIMARY KEY (TAG, COHORT_DEFINITION_ID)
);

INSERT INTO @database_schema.@table_prefix@cse_cohort_tag (tag, cohort_definition_id)
SELECT
    'ATC' as TAG,
    COHORT_DEFINITION_ID
FROM @database_schema.@table_prefix@cohort_definition cd
WHERE cd.ATC_FLAG = 1;

INSERT INTO @database_schema.@table_prefix@cse_cohort_tag (tag, cohort_definition_id)
SELECT
    'RxNorm' as TAG,
    COHORT_DEFINITION_ID
FROM @database_schema.@table_prefix@cohort_definition cd
WHERE cd.ATC_FLAG = 0;

DROP TABLE IF EXISTS @database_schema.@table_prefix@cohort_definition;
DROP TABLE IF EXISTS @database_schema.@table_prefix@atc_level;
