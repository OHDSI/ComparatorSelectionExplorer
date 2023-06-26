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

#' Generate similarity scores
#' @description create cosine similarity scores
#' @inheritParams execute
#' @export
generateSimilarityScores <- function(executionSettings = NULL, ...) {
  if (is.null(executionSettings) || missing(executionSettings)) {
    executionSettings <- createExecutionSettings(..., .callbackFun = on.exit)
  }

  sql <- SqlRender::loadRenderTranslateSql("SelectiveFeatureExtraction.sql",
                                           packageName = utils::packageName(),
                                           dbms = DatabaseConnector::dbms(executionSettings$connection),
                                           cohort_counts = executionSettings$cohortCountTable,
                                           cohort = executionSettings$cohortTableNames$cohortTable,
                                           cdm_database_schema = executionSettings$cdmDatabaseSchema,
                                           results_database_schema = executionSettings$resultsDatabaseSchema,
                                           covariate_def_table = executionSettings$covariateDefTable,
                                           covariate_means_table = executionSettings$covariateMeansTable,
                                           cohort_database_schema = executionSettings$cohortDatabaseSchema,
                                           tempEmulationSchema = executionSettings$tempEmulationSchema)
  DatabaseConnector::executeSql(executionSettings$connection, sql)

  sql <- SqlRender::loadRenderTranslateSql("CosineSimilarity.sql",
                                           packageName = utils::packageName(),
                                           dbms = DatabaseConnector::dbms(executionSettings$connection),
                                           cohort_definition = executionSettings$cohortDefinitionTable,
                                           cdm_database_schema = executionSettings$cdmDatabaseSchema,
                                           results_database_schema = executionSettings$resultsDatabaseSchema,
                                           tempEmulationSchema = executionSettings$tempEmulationSchema,
                                           cohort_counts = executionSettings$cohortCountsTable,
                                           covariate_def_table = executionSettings$covariateDefTable,
                                           covariate_means_table = executionSettings$covariateMeansTable,
                                           cosine_sim_table_2 = executionSettings$cosineSimStratifiedTable,
                                           target_cohort_ids = executionSettings$targetCohortIds)

  DatabaseConnector::executeSql(executionSettings$connection, sql)
  executionSettings$cosineSimilarityExecuted <- TRUE
  invisible(executionSettings)
}
