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

  if (!is.null(executionSettings$cohortDefinitionSet)) {
    # Generate custom cohorts
    ParallelLogger::logInfo("Creating custom cohorts with Cohort Generator")
    if (executionSettings$generateCohortDefinitionSet) {
      CohortGenerator::generateCohortSet(connection = executionSettings$connection,
                                         cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
                                         tempEmulationSchema = executionSettings$tempEmulationSchema,
                                         cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
                                         cohortTableNames = executionSettings$cohortTableNames,
                                         cohortDefinitionSet = executionSettings$cohortDefinitionSet,
                                         stopOnError = TRUE,
                                         incremental = TRUE,
                                         incrementalFolder = executionSettings$incrementalFolder)
    }

    cohortRef <- executionSettings$generateCohortDefinitionSet %>%
      dplyr::select("cohortId", "cohortName") %>%
      dplyr::mutate(atcFlag = -1,
                    conceptId = -1,
                    shortName = cohortName,
                    isCustomCohort = 1) %>%
      dplyr::rename("cohortDefinitionName" = "cohortName",
                    "cohortDefinitionId" = "cohortId")

    DatabaseConnector::insertTable(connection = executionSettings$connection,
                                   data = cohortRef,
                                   tableName = executionSettings$cohortDefinitionTable,
                                   databaseSchema = executionSettings$resultsDatabaseSchema,
                                   camelCaseToSnakeCase = TRUE,
                                   dropTableIfExists = FALSE,
                                   createTable = FALSE,
                                   tempTable = FALSE)
  }

  invisible(executionSettings)
}
