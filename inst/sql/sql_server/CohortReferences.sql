{DEFAULT @cohort_definition = cse_cohort_definition}
DROP TABLE IF EXISTS @results_database_schema.@cohort_definition;

CREATE TABLE @results_database_schema.@cohort_definition
AS SELECT DISTINCT
    CONCEPT_ID AS COHORT_DEFINITION_ID,
    CONCEPT_ID AS SUBSET_PARENT,
    CONCEPT_NAME AS COHORT_DEFINITION_NAME,
    CONCAT(VOCABULARY_ID, ' - ', CONCEPT_NAME) AS SHORT_NAME,
    CONCEPT_ID,
    CASE WHEN vocabulary_id = 'ATC' THEN 1 else 0 END AS ATC_FLAG
FROM @vocabulary_schema.concept
WHERE (concept_class_id = 'Ingredient' AND vocabulary_id = 'RxNorm' AND standard_concept = 'S')
OR (concept_class_id = 'ATC 4th' AND vocabulary_id = 'ATC');