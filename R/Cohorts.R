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


getExposureCohortDefinitionSet <- function(executionSettings = NULL, ...) {
  if (is.null(executionSettings) || missing(executionSettings)) {
    executionSettings <- createExecutionSettings(..., .callbackFun = on.exit)
  }
  # Get cohort references in a manner that can be used for subsetting operations
  sql <- "SELECT
    COHORT_DEFINITION_ID as cohort_id,
    SHORT_NAME as cohort_name,
    CONCAT('SELECT ', COHORT_DEFINITION_ID) as SQL,
    CONCAT('[', COHORT_DEFINITION_ID, ']') as JSON
    FROM @results_database_schema.@cohort_definition"

  DatabaseConnector::renderTranslateQuerySql(executionSettings$connection,
                                             sql,
                                             results_database_schema = executionSettings$resultsDatabaseSchema,
                                             tempEmulationSchema = executionSettings$tempEmulationSchema,
                                             cohort_definition = executionSettings$cohortDefinitionTable,
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

  # Create tables if they don't exist already
  ParallelLogger::logInfo("Creating cohort tables")
  CohortGenerator::createCohortTables(connection = executionSettings$connection,
                                      cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
                                      cohortTableNames = executionSettings$cohortTableNames,
                                      incremental = TRUE)

  # Create reference table
  ParallelLogger::logInfo("Creating bulk cohort reference table")
  sql <- SqlRender::loadRenderTranslateSql("CohortReferences.sql",
                                           packageName = utils::packageName(),
                                           dbms = DatabaseConnector::dbms(executionSettings$connection),
                                           cohort_definition = executionSettings$cohortDefinitionTable,
                                           results_database_schema = executionSettings$resultsDatabaseSchema,
                                           tempEmulationSchema = executionSettings$tempEmulationSchema,
                                           vocabulary_schema = executionSettings$vocabularyDatabaseSchema)
  DatabaseConnector::executeSql(executionSettings$connection, sql)

  # Generate cohorts
  ParallelLogger::logInfo("Creating bulk cohorts")
  sql <- SqlRender::loadRenderTranslateSql("CreateCohorts.sql",
                                           packageName = utils::packageName(),
                                           dbms = DatabaseConnector::dbms(executionSettings$connection),
                                           reference_schema = executionSettings$resultsDatabaseSchema,
                                           cohort_table = executionSettings$cohortTableNames$cohortTable,
                                           cohort_definition = executionSettings$cohortDefinitionTable,
                                           cdm_database_schema = executionSettings$cdmDatabaseSchema,
                                           vocabulary_database_schema = executionSettings$vocabularyDatabaseSchema,
                                           tempEmulationSchema = executionSettings$tempEmulationSchema,
                                           cohort_database_schema = executionSettings$cohortDatabaseSchema)
  DatabaseConnector::executeSql(executionSettings$connection, sql)

  baseCohortSet <- getExposureCohortDefinitionSet(executionSettings)
  # Write cohorts complete to prevent cohort generator creating them
  recordKeepingFile <- file.path(executionSettings$incrementalFolder, "GeneratedCohorts.csv")
  for (i in seq_along(baseCohortSet$cohortId)) {
    CohortGenerator::recordTasksDone(cohortId = baseCohortSet$cohortId[i],
                                     checksum = CohortGenerator::computeChecksum(baseCohortSet$sql[i]),
                                     recordKeepingFile = recordKeepingFile)
  }

  mergedCohortDefinitionSet <- dplyr::bind_rows(executionSettings$cohortDefinitionSet,
                                                baseCohortSet)

  # Add subeset to cohort definition set
  for (subsetDef in executionSettings$indicationCohortSubsetDefintions) {
    mergedCohortDefinitionSet <- mergedCohortDefinitionSet %>%
      CohortGenerator::addCohortSubsetDefinition(subsetDef)
  }

  # Generate custom cohorts
  ParallelLogger::logInfo("Creating custom cohorts with Cohort Generator")
  CohortGenerator::generateCohortSet(connection = executionSettings$connection,
                                     cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
                                     tempEmulationSchema = executionSettings$tempEmulationSchema,
                                     cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
                                     cohortTableNames = executionSettings$cohortTableNames,
                                     cohortDefinitionSet = mergedCohortDefinitionSet,
                                     stopOnError = TRUE,
                                     incremental = TRUE,
                                     incrementalFolder = executionSettings$incrementalFolder)

  # Run subsets on
  cohortRef <- mergedCohortDefinitionSet %>%
    dplyr::filter(!(.data$cohortId %in% baseCohortSet$cohortId)) %>%
    dplyr::select("cohortId", "cohortName", "subsetParent") %>%
    dplyr::mutate(atcFlag = -1,
                  conceptId = -1,
                  shortName = .data$cohortName) %>%
    dplyr::rename("cohortDefinitionName" = "cohortName",
                  "cohortDefinitionId" = "cohortId")

  colnames(cohortRef) <- toupper(SqlRender::camelCaseToSnakeCase(colnames(cohortRef)))
  DatabaseConnector::insertTable(connection = executionSettings$connection,
                                 data = cohortRef,
                                 tableName = executionSettings$cohortDefinitionTable,
                                 databaseSchema = executionSettings$resultsDatabaseSchema,
                                 camelCaseToSnakeCase = FALSE,
                                 dropTableIfExists = FALSE,
                                 createTable = FALSE,
                                 tempTable = FALSE)

  invisible(executionSettings)
}
