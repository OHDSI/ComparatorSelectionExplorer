# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of CohortGenerator
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' @title get exposure cohort definition set
#' @description
#' Returns set of base bulk cohorts with dummy json and sql.
#' This is mainly useful for inspecting which cohorts to subset
#' @inheritParams execute
#' @param includeCounts include cohort counts with definitions  (will fail if they are yet to be instantiated)ß
getExposureCohortDefinitionSet <- function(executionSettings = NULL, includeCounts = FALSE, ...) {
  if (is.null(executionSettings) || missing(executionSettings)) {
    executionSettings <- createExecutionSettings(..., .callbackFun = on.exit)
  }

  # Get cohort references in a manner that can be used for subsetting operations
  sql <- "
  SELECT
    cd.COHORT_DEFINITION_ID as cohort_id,
    cd.SHORT_NAME as cohort_name,
    {@include_counts} ? {
    CASE
      WHEN cc.cohort_definition_id IS NULL THEN 0
      ELSE cc.num_persons
    END AS num_persons
    ,}
    CONCAT('SELECT ', cd.COHORT_DEFINITION_ID, ';') as SQL,
    CONCAT('[', cd.COHORT_DEFINITION_ID, ']') as JSON
    FROM @results_database_schema.@cohort_definition cd
    {@include_counts} ? {
    LEFT JOIN (
        select count(distinct subject_id) as num_persons, sc1.cohort_definition_id
	    from @cohort_database_schema.@cohort sc1
	    group by sc1.cohort_definition_id
    ) cc ON cd.cohort_definition_id = cc.cohort_definition_id
    }
    "

  DatabaseConnector::renderTranslateQuerySql(executionSettings$connection,
                                             sql,
                                             results_database_schema = executionSettings$resultsDatabaseSchema,
                                             cohort_database_schema = executionSettings$cohortDatabaseSchema,
                                             tempEmulationSchema = executionSettings$tempEmulationSchema,
                                             cohort_definition = executionSettings$cohortDefinitionTable,
                                             cohort = executionSettings$cohortTableNames$cohortTable,
                                             include_counts = includeCounts,
                                             snakeCaseToCamelCase = TRUE)
}


#' @title create cohorts
#' @description Create cohorts
#' @inheritParams execute
#' @export
createCohorts <- function(executionSettings = NULL, ...) {
  if (is.null(executionSettings) || missing(executionSettings)) {
    executionSettings <- createExecutionSettings(..., .callbackFun = on.exit)
  }

  if (is.null(executionSettings$cohortDefinitionSet) & !executionSettings$useBulkCohorts) {
    # Use just the RxNorm and atc cohort template definitions
    abort("Must use either custom cohorts or bulk cohorts")
  }

  if (is.null(executionSettings$cohortDefinitionSet)) {
    executionSettings$cohortDefinitionSet <-
      CohortGenerator::createEmptyCohortDefinitionSet()
  }

  if (executionSettings$useBulkCohorts) {
    executionSettings$cohortDefinitionSet <- executionSettings$cohortDefinitionSet |>
      CohortGenerator::createRxNormCohortTemplateDefinition(connection = executionSettings$connection,
                                                            cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
                                                            cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
                                                            tempEmulationSchema = executionSettings$tempEmulationSchema) |>
      CohortGenerator::createAtcCohortTemplateDefinition(connection = executionSettings$connection,
                                                         cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
                                                         cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
                                                         tempEmulationSchema = executionSettings$tempEmulationSchema)
  }


  purrr::walk(executionSettings$indicationCohortSubsetDefintions, function(subsetDef) {
     executionSettings$cohortDefinitionSet <<- executionSettings$cohortDefinitionSet |>
       CohortGenerator::addCohortSubsetDefinition(subsetDef)
  })

  CohortGenerator::generateCohortSet(connection = executionSettings$connection,
                                     cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
                                     tempEmulationSchema = executionSettings$tempEmulationSchema,
                                     cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
                                     cohortTableNames = executionSettings$cohortTableNames,
                                     cohortDefinitionSet = executionSettings$cohortDefinitionSet,
                                     stopOnError = TRUE,
                                     incremental = TRUE,
                                     incrementalFolder = executionSettings$incrementalFolder)

  executionSettings$cohortsGenerated <- TRUE
  invisible(executionSettings)
}
