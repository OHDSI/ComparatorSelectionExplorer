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


.getFeaturesSql <- function(executionSettings, dbms = DatabaseConnector::dbms(executionSettings$connection)) {
  sql <- SqlRender::loadRenderTranslateSql("SelectiveFeatureExtraction.sql",
                                           packageName = utils::packageName(),
                                           dbms = dbms,
                                           cohort_counts = executionSettings$cohortCountTable,
                                           cohort = executionSettings$cohortTableNames$cohortTable,
                                           cdm_database_schema = executionSettings$cdmDatabaseSchema,
                                           results_database_schema = executionSettings$resultsDatabaseSchema,
                                           covariate_def_table = executionSettings$covariateDefTable,
                                           covariate_means_table = executionSettings$covariateMeansTable,
                                           cohort_database_schema = executionSettings$cohortDatabaseSchema,
                                           tempEmulationSchema = executionSettings$tempEmulationSchema) |>
    as.character()

  return(sql)
}


.getCosineSimilaritySql <- function(executionSettings, hasCohortTags = FALSE, dbms = DatabaseConnector::dbms(executionSettings$connection)) {
  sql <- SqlRender::loadRenderTranslateSql("CosineSimilarity.sql",
                                           packageName = utils::packageName(),
                                           dbms = dbms,
                                           cohort_definition = executionSettings$cohortDefinitionTable,
                                           cdm_database_schema = executionSettings$cdmDatabaseSchema,
                                           results_database_schema = executionSettings$resultsDatabaseSchema,
                                           tempEmulationSchema = executionSettings$tempEmulationSchema,
                                           cohort_counts = executionSettings$cohortCountsTable,
                                           covariate_def_table = executionSettings$covariateDefTable,
                                           covariate_means_table = executionSettings$covariateMeansTable,
                                           cosine_sim_table_2 = executionSettings$cosineSimStratifiedTable,
                                           target_cohort_ids = executionSettings$targetCohortIds,
                                           cohort_tags = hasCohortTags) |>
    as.character()
  return(sql)
}


#' Generate similarity scores
#' @description
#' create cosine similarity scores
#' @inheritParams
#' execute
#' @export
generateSimilarityScores <- function(executionSettings = NULL, ...) {
  if (is.null(executionSettings) || missing(executionSettings)) {
    executionSettings <- createExecutionSettings(...)
  }

  ParallelLogger::logInfo("Generating similarity scores")
  sql <- .getFeaturesSql(executionSettings)

  DatabaseConnector::executeSql(executionSettings$connection, sql)

  ParallelLogger::logInfo("Computing cosine similarity")
  
  # Create temp table for cohort tags if tags are specified
  hasCohortTags <- FALSE
  if (!is.null(executionSettings$cohortTags)) {
    ParallelLogger::logInfo("Creating temporary table for cohort tags")
    hasCohortTags <- cohortTagsToTempTable(executionSettings$cohortTags, executionSettings$connection)
  }
  
  sql <- .getCosineSimilaritySql(executionSettings, hasCohortTags = hasCohortTags)

  DatabaseConnector::executeSql(executionSettings$connection, sql)
  
  # Clean up temp table if created
  if (hasCohortTags) {
    DatabaseConnector::renderTranslateExecuteSql(executionSettings$connection, "DROP TABLE IF EXISTS #cse_cohort_tags;")
  }
  
  executionSettings$cosineSimilarityExecuted <- TRUE
  invisible(executionSettings)
}

