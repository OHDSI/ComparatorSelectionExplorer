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

#' @title create cohorts
#' @description Create cohorts
#' @inheritParams execute
#' @export
createCohorts <- function(executionSettings = NULL, ...) {
  if (is.null(executionSettings) || missing(executionSettings)) {
    executionSettings <- createExecutionSettings(...)
  }

  if (is.null(executionSettings$cohortDefinitionSet) & !executionSettings$useBulkCohorts) {
    # Use just the RxNorm and atc cohort template definitions
    stop("Must use either custom cohorts bulk cohorts or both")
  }

  if (is.null(executionSettings$cohortDefinitionSet)) {
    executionSettings$cohortDefinitionSet <-
      CohortGenerator::createEmptyCohortDefinitionSet()
  }


  cohrtRef <- data.frame()
  if (executionSettings$useBulkCohorts) {
    rxNormTpl <- CohortGenerator::createRxNormCohortTemplateDefinition(connection = executionSettings$connection,
                                                                       cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
                                                                       cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
                                                                       tempEmulationSchema = executionSettings$tempEmulationSchema)

    rxNormRefs <- rxNormTpl$references

    cohrtRef <- cohrtRef |>
      dplyr::bind_rows(
        rxNormRefs |>
          dplyr::select("cohortId", "cohortName") |>
          dplyr::mutate(atcFlag = 0,
                        conceptId = .data$cohortId / 1000,
                        shortName = .data$cohortName) |>
          dplyr::rename("cohortDefinitionName" = "cohortName",
                        "cohortDefinitionId" = "cohortId")
      )

    executionSettings$cohortDefinitionSet <- executionSettings$cohortDefinitionSet |>
      CohortGenerator::addCohortTemplateDefintion(rxNormTpl)

    atcTpl <- CohortGenerator::createAtcCohortTemplateDefinition(connection = executionSettings$connection,
                                                                 cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
                                                                 cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
                                                                 tempEmulationSchema = executionSettings$tempEmulationSchema)

    atcRefs <- atcTpl$references

    cohrtRef <- cohrtRef |>
      dplyr::bind_rows(
        atcRefs |>
          dplyr::select("cohortId", "cohortName") |>
          dplyr::mutate(atcFlag = 1,
                        conceptId = .data$cohortId / 1000,
                        shortName = .data$cohortName) |>
          dplyr::rename("cohortDefinitionName" = "cohortName",
                        "cohortDefinitionId" = "cohortId")
      )

    executionSettings$cohortDefinitionSet <- executionSettings$cohortDefinitionSet |>
      CohortGenerator::addCohortTemplateDefintion(atcTpl)
  }

  for(subsetDef in executionSettings$indicationCohortSubsetDefintions) {
    executionSettings$cohortDefinitionSet <- executionSettings$cohortDefinitionSet |>
      CohortGenerator::addCohortSubsetDefinition(subsetDef, targetCohortIds = cohrtRef$cohortDefinitionId)
  }

  if (!"subsetParent" %in% colnames(executionSettings$cohortDefinitionSet)) {
    executionSettings$cohortDefinitionSet$subsetParent <- executionSettings$cohortDefinitionSet$cohortId
    executionSettings$cohortDefinitionSet$isSubset <- FALSE
  }

  CohortGenerator::createCohortTables(connection = executionSettings$connection,
                                      cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
                                      cohortTableNames = executionSettings$cohortTableNames,
                                      incremental = TRUE)

  CohortGenerator::generateCohortSet(connection = executionSettings$connection,
                                     cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
                                     tempEmulationSchema = executionSettings$tempEmulationSchema,
                                     cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
                                     cohortTableNames = executionSettings$cohortTableNames,
                                     cohortDefinitionSet = executionSettings$cohortDefinitionSet,
                                     stopOnError = TRUE,
                                     incremental = TRUE,
                                     incrementalFolder = executionSettings$incrementalFolder)
  # # Insert cohort definition table
  cohrtRef <-
    cohrtRef |> dplyr::bind_rows(
      executionSettings$cohortDefinitionSet |>
        dplyr::filter((.data$isSubset | !.data$isTemplatedCohort) & !.data$cohortId %in% cohrtRef$cohortId) |>
        dplyr::select("cohortId", "cohortName", "subsetParent") |>
        dplyr::mutate(atcFlag = -1,
                      conceptId = -1,
                      shortName = .data$cohortName) |>
        dplyr::rename("cohortDefinitionName" = "cohortName",
                      "cohortDefinitionId" = "cohortId")
    )

  ParallelLogger::logInfo("Inserting cohort references")
  # Create refrences - force casting of value
  tableSql <- "
  DROP TABLE IF EXISTS @schema.@cohort_definition_table;
  CREATE TABLE @schema.@cohort_definition_table (
      cohort_definition_id bigint,
      cohort_definition_name varchar,
      short_name varchar,
      concept_id bigint,
      atc_flag int,
      subset_parent bigint
    );
   "

  DatabaseConnector::renderTranslateExecuteSql(executionSettings$connection,
                                               sql = tableSql,
                                               cohort_definition_table = executionSettings$cohortDefinitionTable,
                                               schema = executionSettings$resultsDatabaseSchema)

  withr::with_options(list(scipen = 9999999), {
    DatabaseConnector::insertTable(connection = executionSettings$connection,
                                   data = cohrtRef,
                                   tableName = executionSettings$cohortDefinitionTable,
                                   databaseSchema = executionSettings$resultsDatabaseSchema,
                                   camelCaseToSnakeCase = TRUE,
                                   dropTableIfExists = FALSE,
                                   createTable = FALSE,
                                   tempTable = FALSE)

  })
  executionSettings$cohortsGenerated <- TRUE
  invisible(executionSettings)
}

