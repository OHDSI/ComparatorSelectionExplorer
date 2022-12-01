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
#' @param databaseId                    Database identifier (string)
#' @param incrementalFolder             folder for storage of incremental results for cohort generation
#' @param vocabularyDatabaseSchema      standard vocabulary database schema
#' @param cohortTable                   (optional) cohort table
#' @param cohortCountTable              (optional) count tabls
#' @param cohortDefinitionTable         (optional) definitions table
#' @param covariateDefTable             (optional) where covariate definitions are stored
#' @param covariateMeansTable           (optional) where covariate means are stored
#' @param cosineSimStratifiedTable      (optional) where stratified cosine similarity scores are stored
#' @param cosineSimTable                (optional) Where cosine similarity scores are stored
#' @param minExposureSize               (optional) Minimum number of exposures to be included in cosine similarity
#'                                      analysis (defaults to 1000).
#' @param logFileLocation               (optional) Log file location
#'
#' @returns executionSettings object
#' @export
createExecutionSettings <- function(connectionDetails,
                                    connection = NULL,
                                    databaseId,
                                    incrementalFolder = paste0("incremental_", databaseId),
                                    cdmDatabaseSchema,
                                    vocabularyDatabaseSchema = cdmDatabaseSchema,
                                    resultsDatabaseSchema,
                                    cohortDatabaseSchema = resultsDatabaseSchema,
                                    cohortTable = "cse_cohort",
                                    tempEmulationSchema = getOption("tempEmulationSchema"),
                                    cohortDefinitionSet = NULL,
                                    cohortCountTable = "cse_cohort_count",
                                    cohortDefinitionTable = "cse_cohort_definition",
                                    covariateDefTable = "cse_covariate_ref",
                                    covariateMeansTable = "cse_covariate_means",
                                    cosineSimStratifiedTable = "cse_cosine_sim_strat",
                                    cosineSimTable = "cse_cosine_sim",
                                    minExposureSize = 1000,
                                    logFileLocation = paste0("cse-execution-log-", databaseId, ".txt"),
                                    exportZipFile) {

  checkmate::assertClass(connectionDetails, "connectionDetails")
  checkmate::assertTRUE(is.null(cohortDefinitionSet) || CohortGenerator::isCohortDefinitionSet(cohortDefinitionSet))

  executionSettings <- list(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    databaseId = databaseId,
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
    cosineSimTable = cosineSimTable,
    minExposureSize = minExposureSize,
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

  return(executionSettings)
}
