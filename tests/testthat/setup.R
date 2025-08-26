connectionDetails <- Eunomia::getEunomiaConnectionDetails()

addFakeAtcVocab <- function(executionSettings) {
  sql <-   sql <- "
  INSERT INTO concept (CONCEPT_ID, CONCEPT_NAME, DOMAIN_ID, VOCABULARY_ID, CONCEPT_CLASS_ID,
                       STANDARD_CONCEPT, CONCEPT_CODE, VALID_START_DATE, VALID_END_DATE)
  SELECT
    21603991 as concept_id, 'Coxibs' as concept_name, 'Drug' as domain_id, 'ATC' as vocabulary_id,
    'ATC 4th' as concept_class_id, 'C' as  standard_concept, 'M01AH' as concept_code,
    '1970-01-01' as valid_start_date, '2099-12-31' as valid_end_date;

  INSERT INTO concept_ancestor (ancestor_concept_id, descendant_concept_id, min_levels_of_separation,
                                max_levels_of_separation)
  SELECT 21603991 as ancestor_concept_id, 1118084 as descendant_concept_id,
         1 as min_levels_of_separation,1  as max_levels_of_separation;
  "
  DatabaseConnector::renderTranslateExecuteSql(executionSettings$connection, sql)
  invisible()
}
