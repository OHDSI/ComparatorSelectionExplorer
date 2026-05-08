# Copyright 2025 Observational Health Data Sciences and Informatics
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

#' @title
#' createExecutionSettings
#' @description
#' executions settings to be used throughout package for analysis
#'
#' @param connectionDetails                  DatabaseConnector::connectionDetails instance
#' @param connection                         DatabaseConnector::connection (defaults to null). Use for
#'                                           persistent storage of reference
#' @param cdmDatabaseSchema                  String Database schema - where data lives
#' @param cohortDatabaseSchema               String Database schema - where cohort table is created
#' @param resultsDatabaseSchema              String Database schema - where similarity scores are
#'                                           stored
#' @param cohortDefinitionSet                CohortGenerator::cohortDefinitionSet - intended to be
#'                                           custom exposures or indication cohorts
#' @param tempEmulationSchema                String DatabaseSchema - temp emulation schema for oracle,
#'                                           bigquery
#' @param exportZipFile                      Path to zip file output of project
#' @param databaseName                       Database identifier (string)
#' @param databaseId                         Database identifier integer (optional)
#' @param cohortTags                         (optional) Named list where names are tag names and values
#'                                           are integer vectors of cohort IDs. Cosine similarity will
#'                                           be calculated only within tag groups (cohorts sharing at
#'                                           least one tag). Example: list("exposure" = c(1,2,3),
#'                                           "outcome" = c(4,5)). If NULL, all cohorts are compared.
#' @param targetCohortIds                    (optional, deprecated) Integer ids for cohorts to limit
#'                                           cosine similarity calculation to. Use cohortTags instead.
#'                                           Must be a valid RxNorm ingredient, ATC class or included
#'                                           in the cohortDefinitionSet
#'
#' @param vocabularyDatabaseSchema           standard vocabulary database schema
#' @param cohortTable                        cohort table for exposures
#' @param cohortCountTable                   (optional) count tabls
#' @param cohortDefinitionTable              (optional) definitions table
#' @param covariateDefTable                  (optional) where covariate definitions are stored
#' @param covariateMeansTable                (optional) where covariate means are stored
#' @param cosineSimStratifiedTable           (optional) where stratified cosine similarity scores are
#'                                           stored]
#' @param exportAtcLevels                    (Optional) export ATC levels from vocabulary. Needed by shiny app for some features but very
#'                                           large and can be avoided on most runs
#' @param minExposureSize                    (optional) Minimum number of exposures to be included in
#'                                           cosine similarity analysis (defaults to 1000).
#' @param logFileLocation                    (optional) Log file location
#' @param exportDir                          (optional) Folder to store results files in before export
#'                                           (default is tempdir)
#' @param removeExportDir                    (optional) remove the export dir after creating zip files?
#' @returns
#' executionSettings object
#' @export
#' @importFrom digest digest2int
createExecutionSettings <- function(connectionDetails = NULL,
                                    connection = NULL,
                                    databaseName = NULL,
                                    databaseId = NULL,
                                    cdmDatabaseSchema,
                                    vocabularyDatabaseSchema = cdmDatabaseSchema,
                                    resultsDatabaseSchema,
                                    cohortDatabaseSchema = resultsDatabaseSchema,
                                    cohortTable,
                                    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                    cohortDefinitionSet = NULL,
                                    cohortTags = NULL,
                                    targetCohortIds = NULL,
                                    cohortCountTable = "cse_cohort_count",
                                    cohortDefinitionTable = "cse_cohort_definition",
                                    covariateDefTable = "cse_covariate_ref",
                                    covariateMeansTable = "cse_covariate_means",
                                    cosineSimStratifiedTable = "cse_cosine_sim",
                                    exportAtcLevels = FALSE,
                                    minExposureSize = 1000,
                                    logFileLocation = paste0("cse-execution-log-", cdmDatabaseSchema, ".txt"),
                                    exportDir = tempfile(),
                                    removeExportDir = TRUE,
                                    exportZipFile = file.path(normalizePath(getwd()), paste0("cse_results_", cdmDatabaseSchema, ".zip"))) {
  checkmate::assert(
    checkmate::checkClass(connectionDetails, "ConnectionDetails", null.ok = TRUE),
    checkmate::checkClass(connection, "DatabaseConnectorConnection", null.ok = TRUE),
    combine = "or"
  )
  if (is.null(connectionDetails) && is.null(connection)) {
    stop("Either connectionDetails or connection must be provided")
  }
  checkmate::assertTRUE(is.null(cohortDefinitionSet) || CohortGenerator::isCohortDefinitionSet(cohortDefinitionSet))
  checkmate::assertIntegerish(databaseId, null.ok = TRUE)
  
  # Backward compatibility: convert targetCohortIds to cohortTags if provided
  if (!is.null(targetCohortIds) && is.null(cohortTags)) {
    ParallelLogger::logWarn("targetCohortIds is deprecated. Please use cohortTags instead.")
    checkmate::assertNumeric(targetCohortIds)
    cohortTags <- convertTargetCohortIdsToTags(targetCohortIds)
  } else if (!is.null(targetCohortIds) && !is.null(cohortTags)) {
    stop("Cannot specify both targetCohortIds and cohortTags. Please use cohortTags only.")
  }
  
  # Validate cohortTags if provided
  if (!is.null(cohortTags)) {
    validateCohortTags(cohortTags)
  }
  
  # Get flattened list of all cohort IDs for filtering
  allTargetCohortIds <- flattenCohortTags(cohortTags)


  executionSettings <- list(connectionDetails = connectionDetails,
                            cdmDatabaseSchema = cdmDatabaseSchema,
                            databaseName = databaseName,
                            vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                            resultsDatabaseSchema = resultsDatabaseSchema,
                            cohortDatabaseSchema = cohortDatabaseSchema,
                            tempEmulationSchema = tempEmulationSchema,
                            exportZipFile = exportZipFile,
                            logFileLocation = logFileLocation,
                            cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable),
                            cohortCountTable = cohortCountTable,
                            cohortDefinitionTable = cohortDefinitionTable,
                            covariateDefTable = covariateDefTable,
                            covariateMeansTable = covariateMeansTable,
                            cosineSimStratifiedTable = cosineSimStratifiedTable,
                            minExposureSize = minExposureSize,
                            exportDir = exportDir,
                            exportAtcLevels = exportAtcLevels,
                            removeExportDir = removeExportDir,
                            cohortDefinitionSet = cohortDefinitionSet,
                            cohortTags = cohortTags,
                            targetCohortIds = allTargetCohortIds,
                            connection = connection)
  class(executionSettings) <- "executionSettings"

  attr(executionSettings,
       ".execStatus") <- list(cohortReferencesCreated = FALSE, simialrityScores = FALSE)

  if (!is.null(logFileLocation)) {
    ParallelLogger::clearLoggers()
    ParallelLogger::addDefaultFileLogger(logFileLocation)
    ParallelLogger::addDefaultConsoleLogger()
  }

  # Get database ID from cdm_source table
  if (is.null(executionSettings$connection)) {
    executionSettings$connection <- DatabaseConnector::connect(executionSettings$connectionDetails)
    on.exit({
      DatabaseConnector::disconnect(executionSettings$connection)
      executionSettings$connection <- NULL
    }, add = TRUE)
  }

  executionSettings$databaseId <- databaseId
  if (is.null(executionSettings$databaseId)) {
    fields <- DatabaseConnector::renderTranslateQuerySql(executionSettings$connection, "
                                                         SELECT
                                                             CDM_SOURCE_NAME,
                                                             CDM_SOURCE_ABBREVIATION,
                                                             SOURCE_RELEASE_DATE,
                                                             CDM_RELEASE_DATE
                                                         FROM @cdm_database_schema.cdm_source;
                                                         ",
                                                         cdm_database_schema = executionSettings$cdmDatabaseSchema,
                                                         snakeCaseToCamelCase = TRUE)

    executionSettings$databaseId <- abs(digest::digest2int(paste(fields, collapse = ""),
                                                           seed = 999))
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
