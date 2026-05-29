# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of ComparatorSelectionExplorer
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

.zipResults <- function(executionSettings) {
  ParallelLogger::logInfo("Creating zip file ", executionSettings$exportZipFile)
  pwd <- getwd()
  on.exit(setwd(pwd))
  setwd(executionSettings$exportDir)
  DatabaseConnector::createZipFile(zipFile = executionSettings$exportZipFile,
                                   files = list.files("./", pattern = "*.csv"))
}


#' @title Export Results
#' @description Create a zip file containing csvs of computed results tables
#' @inheritParams execute
#' @export
exportResults <- function(executionSettings = NULL, ...) {

  if (is.null(executionSettings) || missing(executionSettings)) {
    executionSettings <- createExecutionSettings(...)
  }

  if (is.null(executionSettings$connection)) {
    executionSettings$connection <- DatabaseConnector::connect(executionSettings$connectionDetails)
    on.exit({
      DatabaseConnector::disconnect(executionSettings$connection)
      executionSettings$connection <- NULL
    })
  }

  if (!dir.exists(executionSettings$exportDir)) {
    dir.create(executionSettings$exportDir, recursive = TRUE)
  }

  exportResultsFun <- function(data, position, csvFilename, addDbId = FALSE, ...) {
    if (addDbId) {
      if (nrow(data)) {
        data$database_id <- executionSettings$databaseId
      } else {
        data <- data %>% dplyr::mutate(database_id = "")
      }
    }

    readr::write_csv(data,
                     file = file.path(executionSettings$exportDir, csvFilename),
                     append = position != 1,
                     na = "")

    invisible(NULL)
  }

  # Is there a subset of cohorts to export data by
  exportByTargetSet <- !is.null(executionSettings$targetCohortIds)

  if (exportByTargetSet) {
    targetSetTable <- data.frame(cohortDefinitionId = executionSettings$targetCohortIds)
    DatabaseConnector::renderTranslateExecuteSql(executionSettings$connection, "DROP TABLE IF EXISTS #cse_target_export;")
    DatabaseConnector::insertTable(executionSettings$connection,
                                   data = targetSetTable,
                                   tableName = "cse_target_export",
                                   camelCaseToSnakeCase = TRUE,
                                   dropTableIfExists = TRUE,
                                   createTable = TRUE,
                                   tempTable = TRUE)

    on.exit({
      DatabaseConnector::renderTranslateExecuteSql(executionSettings$connection, "DROP TABLE IF EXISTS #cse_target_export;")
    }, add = TRUE)
  }

  ParallelLogger::logInfo("Exporting covariate def table")
  DatabaseConnector::renderTranslateQueryApplyBatched(executionSettings$connection,
                                                      "SELECT * FROM  @results_database_schema.@covariate_def_table",
                                                      fun = exportResultsFun,
                                                      args = list(
                                                        csvFilename = "cse_covariate_definition.csv",
                                                        addDbId = FALSE
                                                      ),
                                                      covariate_def_table = executionSettings$covariateDefTable,
                                                      results_database_schema = executionSettings$resultsDatabaseSchema)
  ParallelLogger::logInfo("Exporting cse_cohort_count")


  sql <- "SELECT ct.* FROM  @results_database_schema.@count_table ct
  {@export_target_set} ? {
  INNER JOIN #cse_target_export ctes ON ctes.cohort_definition_id = ct.cohort_definition_id
  }
  WHERE ct.num_persons >= @min_exposure_size"
  DatabaseConnector::renderTranslateQueryApplyBatched(executionSettings$connection,
                                                      sql,
                                                      fun = exportResultsFun,
                                                      args = list(
                                                        csvFilename = "cse_cohort_count.csv",
                                                        addDbId = TRUE
                                                      ),
                                                      export_target_set = exportByTargetSet,
                                                      count_table = executionSettings$cohortCountTable,
                                                      min_exposure_size = executionSettings$minExposureSize,
                                                      results_database_schema = executionSettings$resultsDatabaseSchema)

  # Export user-specified cohort tags
  if (!is.null(executionSettings$cohortTags)) {
    ParallelLogger::logInfo("Exporting cohort tags")
    tagsDataFrame <- cohortTagsToDataFrame(executionSettings$cohortTags)
    colnames(tagsDataFrame) <- tolower(SqlRender::camelCaseToSnakeCase(colnames(tagsDataFrame)))
    readr::write_csv(tagsDataFrame,
                     file = file.path(executionSettings$exportDir, "cse_cohort_tag.csv"),
                     na = "")
  }

  if (isTRUE(executionSettings$exportAtcLevels)) {
    ParallelLogger::logInfo("Exporting cse_atc_level")

    sql <- SqlRender::readSql(system.file(file.path("sql", "sql_server", "GetAtcLevels.sql"),
                                        package = "ComparatorSelectionExplorer"))
    DatabaseConnector::renderTranslateQueryApplyBatched(executionSettings$connection,
                                                        sql,
                                                        fun = exportResultsFun,
                                                        args = list(
                                                          csvFilename = "cse_atc_level.csv",
                                                          addDbId = FALSE
                                                        ),
                                                        vocabulary_database_schema = executionSettings$vocabularyDatabaseSchema)
  }
  sql <- "
  SELECT t.* FROM  @results_database_schema.@table t
  INNER JOIN @results_database_schema.@count_table ct ON t.cohort_definition_id = ct.cohort_definition_id
  {@export_target_set} ? {
  INNER JOIN #cse_target_export ctes ON ctes.cohort_definition_id = ct.cohort_definition_id
  }
  WHERE ct.num_persons >= @min_exposure_size"

  ParallelLogger::logInfo("Exporting cse_covariate_mean")
  DatabaseConnector::renderTranslateQueryApplyBatched(executionSettings$connection,
                                                      sql,
                                                      fun = exportResultsFun,
                                                      args = list(
                                                        csvFilename = "cse_covariate_mean.csv",
                                                        addDbId = TRUE
                                                      ),
                                                      count_table = executionSettings$cohortCountTable,
                                                      min_exposure_size = executionSettings$minExposureSize,
                                                      export_target_set = exportByTargetSet,
                                                      table = executionSettings$covariateMeansTable,
                                                      results_database_schema = executionSettings$resultsDatabaseSchema)

  sql <- "
  SELECT t.* FROM @results_database_schema.@table t
  INNER JOIN @results_database_schema.@count_table ct ON t.cohort_definition_id_1 = ct.cohort_definition_id
  INNER JOIN @results_database_schema.@count_table ct2 ON t.cohort_definition_id_2 = ct2.cohort_definition_id
  {@export_target_set} ? {
  INNER JOIN #cse_target_export ctes ON ctes.cohort_definition_id = ct.cohort_definition_id
  INNER JOIN #cse_target_export ctes2 ON ctes2.cohort_definition_id = ct2.cohort_definition_id
  }
  WHERE ct.num_persons >= @min_exposure_size
  AND ct2.num_persons >= @min_exposure_size
  "
  ParallelLogger::logInfo("Exporting cse_cosine_similarity_score")
  DatabaseConnector::renderTranslateQueryApplyBatched(executionSettings$connection,
                                                      sql,
                                                      fun = exportResultsFun,
                                                      args = list(
                                                        csvFilename = "cse_cosine_similarity_score.csv",
                                                        addDbId = TRUE
                                                      ),
                                                      count_table = executionSettings$cohortCountTable,
                                                      min_exposure_size = executionSettings$minExposureSize,
                                                      table = executionSettings$cosineSimStratifiedTable,
                                                      export_target_set = exportByTargetSet,
                                                      results_database_schema = executionSettings$resultsDatabaseSchema)

  sql <- "SELECT * FROM @cdm_database_schema.cdm_source"

  ParallelLogger::logInfo("Exporting cse_cdm_source_info")
  DatabaseConnector::renderTranslateQueryApplyBatched(executionSettings$connection,
                                                      sql,
                                                      fun = exportResultsFun,
                                                      args = list(
                                                        csvFilename = "cse_cdm_source_info.csv",
                                                        addDbId = TRUE
                                                      ),
                                                      cdm_database_schema = executionSettings$cdmDatabaseSchema)
  .zipResults(executionSettings)

  if (executionSettings$removeExportDir) {
    ParallelLogger::logInfo("Removing export directory ", executionSettings$exportDir)
    unlink(executionSettings$exportDir, recursive = TRUE, force = TRUE)
  }

  executionSettings$resultsExported <- TRUE
  invisible(executionSettings)
}
