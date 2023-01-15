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

#' @title createExecutionSettings
#' @description executions settings to be used throughout package for analysis
#'
#' @param connectionDetails             DatabaseConnector::connectionDetails instance
#' @param connection                    DatabaseConnector::connection (defaults to null). Use for persistent storage of
#'                                      reference
#' @param cdmDatabaseSchema             String Database schema - where data lives
#' @param cohortDatabaseSchema          String Database schema - where cohort table is created
#' @param resultsDatabaseSchema         String Database schema - where similarity scores are stored
#' @param cohortDefinitionSet           CohortGenerator::cohortDefinitionSet
#' @param tempEmulationSchema           String DatabaseSchema - temp emulation schema for oracle, bigquery
#' @param exportZipFile                 Path to zip file output of project
#' @param databaseName                  Database identifier (string)
#' @param databaseId                    Database identifier integer (optional)
#' @param incrementalFolder             folder for storage of incremental results for cohort generation
#' @param vocabularyDatabaseSchema      standard vocabulary database schema
#' @param cohortTable                   (optional) cohort table
#' @param cohortCountTable              (optional) count tabls
#' @param cohortDefinitionTable         (optional) definitions table
#' @param covariateDefTable             (optional) where covariate definitions are stored
#' @param covariateMeansTable           (optional) where covariate means are stored
#' @param cosineSimStratifiedTable      (optional) where stratified cosine similarity scores are stored
#' @param minExposureSize               (optional) Minimum number of exposures to be included in cosine similarity
#'                                      analysis (defaults to 1000).
#' @param logFileLocation               (optional) Log file location
#' @param exportDir                     (optional) Folder to store results files in before export (default is tempdir)
#' @param removeExportDir               (optional) remove the export dir after creating zip files?
#' @pram .callbackFun                   Used internally - an on.exit call for disconnection from db
#' @returns executionSettings object
#' @export
createExecutionSettings <- function(connectionDetails,
                                    connection = NULL,
                                    databaseName = NULL,
                                    databaseId = NULL,
                                    cdmDatabaseSchema,
                                    vocabularyDatabaseSchema = cdmDatabaseSchema,
                                    incrementalFolder = paste0("incremental_", cdmDatabaseSchema),
                                    resultsDatabaseSchema,
                                    cohortDatabaseSchema = resultsDatabaseSchema,
                                    cohortTable = "cse_cohort",
                                    tempEmulationSchema = getOption("tempEmulationSchema"),
                                    cohortDefinitionSet = NULL,
                                    cohortCountTable = "cse_cohort_count",
                                    cohortDefinitionTable = "cse_cohort_definition",
                                    covariateDefTable = "cse_covariate_ref",
                                    covariateMeansTable = "cse_covariate_means",
                                    cosineSimStratifiedTable = "cse_cosine_sim",
                                    minExposureSize = 1000,
                                    logFileLocation = paste0("cse-execution-log-", cdmDatabaseSchema, ".txt"),
                                    exportDir = tempfile(),
                                    removeExportDir = TRUE,
                                    generateCohortDefinitionSet = FALSE,
                                    exportZipFile = file.path(normalizePath(getwd()), paste0("cse_results_", cdmDatabaseSchema, ".zip")),
                                    .callbackFun = NULL) {

  checkmate::assertClass(connectionDetails, "ConnectionDetails")
  checkmate::assertTRUE(is.null(cohortDefinitionSet) || CohortGenerator::isCohortDefinitionSet(cohortDefinitionSet))
  checkmate::assertIntegerish(databaseId, null.ok = TRUE)
  checkmate::assertString(databaseId, null.ok = TRUE)

  executionSettings <- list(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    databaseName = databaseName,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    resultsDatabaseSchema = resultsDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    exportZipFile = exportZipFile,
    incrementalFolder = incrementalFolder,
    logFileLocation = logFileLocation,
    cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable),
    cohortCountTable = cohortCountTable,
    cohortDefinitionTable = cohortDefinitionTable,
    covariateDefTable = covariateDefTable,
    covariateMeansTable = covariateMeansTable,
    cosineSimStratifiedTable = cosineSimStratifiedTable,
    minExposureSize = minExposureSize,
    exportDir = exportDir,
    removeExportDir = removeExportDir,
    cohortDefinitionSet = cohortDefinitionSet,
    generateCohortDefinitionSet = generateCohortDefinitionSet,
    connection = connection
  )
  class(executionSettings) <- "executionSettings"

  attr(executionSettings, ".execStatus") <- list(cohortReferencesCreated = FALSE,
                                                 cohortsCreated = FALSE,
                                                 simialrityScores = FALSE)

  if (!is.null(logFileLocation)) {
    ParallelLogger::clearLoggers()
    ParallelLogger::addDefaultFileLogger(logFileLocation)
    ParallelLogger::addDefaultConsoleLogger()
  }

  # Get database ID from cdm_source table
  if (is.null(executionSettings$connection)) {
    executionSettings$connection <- DatabaseConnector::connect(executionSettings$connectionDetails)
    if (is.function(.callbackFun)) {
      .callbackFun({
        DatabaseConnector::disconnect(executionSettings$connection)
        executionSettings$connection <- NULL
      })
    }
  }

  executionSettings$databaseId <- databaseId
  if (is.null(executionSettings$databaseId)) {
    fields <- DatabaseConnector::renderTranslateQuerySql(executionSettings$connection,
                                                         "
                                                         SELECT
                                                             CDM_SOURCE_NAME,
                                                             CDM_SOURCE_ABBREVIATION,
                                                             SOURCE_RELEASE_DATE,
                                                             CDM_RELEASE_DATE
                                                         FROM @cdm_database_schema.cdm_source;
                                                         ",
                                                         cdm_database_schema = executionSettings$cdmDatabaseSchema,
                                                         snakeCaseToCamelCase = TRUE)

    executionSettings$databaseId <- abs(digest::digest2int(paste(fields, collapse = ""), seed = 999))
  }

  if (is.null(executionSettings$databaseName)) {
    fields <- DatabaseConnector::renderTranslateQuerySql(executionSettings$connection,
                                                         "SELECT CDM_SOURCE_NAME FROM @cdm_database_schema.cdm_source;",
                                                         cdm_database_schema = executionSettings$cdmDatabaseSchema,
                                                         snakeCaseToCamelCase = TRUE)
    executionSettings$databaseName <- fields$cdmSourceName
  }

  return(executionSettings)
}
