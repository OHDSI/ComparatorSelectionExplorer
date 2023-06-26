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

.zipResults <- function(executionSettings) {
  message("Creating zip file ", executionSettings$exportZipFile)
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
    dir.create(executionSettings$exportDir)
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

  DatabaseConnector::renderTranslateQueryApplyBatched(executionSettings$connection,
                                                      "SELECT * FROM  @results_database_schema.@covariate_def_table",
                                                      fun = exportResultsFun,
                                                      args = list(
                                                        csvFilename = "covariate_definition.csv",
                                                        addDbId = FALSE
                                                      ),
                                                      covariate_def_table = executionSettings$covariateDefTable,
                                                      results_database_schema = executionSettings$resultsDatabaseSchema)


  sql <- "SELECT * FROM  @results_database_schema.@count_table ct WHERE ct.num_persons >= @min_exposure_size"
  DatabaseConnector::renderTranslateQueryApplyBatched(executionSettings$connection,
                                                      sql,
                                                      fun = exportResultsFun,
                                                      args = list(
                                                        csvFilename = "cohort_count.csv",
                                                        addDbId = TRUE
                                                      ),
                                                      count_table = executionSettings$cohortCountTable,
                                                      min_exposure_size = executionSettings$minExposureSize,
                                                      results_database_schema = executionSettings$resultsDatabaseSchema)

  sql <- "
  SELECT
    t.cohort_definition_id,
    t.COHORT_DEFINITION_NAME,
    t.SHORT_NAME,
    t.CONCEPT_ID,
    t.ATC_FLAG,
    t.subset_parent
    FROM  @results_database_schema.@table t
  INNER JOIN @results_database_schema.@count_table ct ON t.cohort_definition_id = ct.cohort_definition_id
  WHERE ct.num_persons >= @min_exposure_size"
  DatabaseConnector::renderTranslateQueryApplyBatched(executionSettings$connection,
                                                      sql,
                                                      fun = exportResultsFun,
                                                      args = list(
                                                        csvFilename = "cohort_definition.csv",
                                                        addDbId = FALSE
                                                      ),
                                                      count_table = executionSettings$cohortCountTable,
                                                      min_exposure_size = executionSettings$minExposureSize,
                                                      table = executionSettings$cohortDefinitionTable,
                                                      results_database_schema = executionSettings$resultsDatabaseSchema)

  sql <- SqlRender::readSql(system.file(file.path("sql", "sql_server", "GetAtcLevels.sql"),
                                        package = utils::packageName()))

  DatabaseConnector::renderTranslateQueryApplyBatched(executionSettings$connection,
                                                      sql,
                                                      fun = exportResultsFun,
                                                      args = list(
                                                        csvFilename = "atc_level.csv",
                                                        addDbId = FALSE
                                                      ),
                                                      table = executionSettings$cohortDefinitionTable,
                                                      vocabulary_database_schema = executionSettings$vocabularyDatabaseSchema,
                                                      results_database_schema = executionSettings$resultsDatabaseSchema)

  sql <- "
  SELECT t.* FROM  @results_database_schema.@table t
  INNER JOIN @results_database_schema.@count_table ct ON t.cohort_definition_id = ct.cohort_definition_id
  WHERE ct.num_persons >= @min_exposure_size"
  DatabaseConnector::renderTranslateQueryApplyBatched(executionSettings$connection,
                                                      sql,
                                                      fun = exportResultsFun,
                                                      args = list(
                                                        csvFilename = "covariate_mean.csv",
                                                        addDbId = TRUE
                                                      ),
                                                      count_table = executionSettings$cohortCountTable,
                                                      min_exposure_size = executionSettings$minExposureSize,
                                                      table = executionSettings$covariateMeansTable,
                                                      results_database_schema = executionSettings$resultsDatabaseSchema)

  sql <- "
  SELECT t.* FROM @results_database_schema.@table t
  INNER JOIN @results_database_schema.@count_table ct ON t.cohort_definition_id_1 = ct.cohort_definition_id
  INNER JOIN @results_database_schema.@count_table ct2 ON t.cohort_definition_id_2 = ct2.cohort_definition_id
  WHERE ct.num_persons >= @min_exposure_size
  AND ct2.num_persons >= @min_exposure_size
  "

  DatabaseConnector::renderTranslateQueryApplyBatched(executionSettings$connection,
                                                      sql,
                                                      fun = exportResultsFun,
                                                      args = list(
                                                        csvFilename = "cosine_similarity_score.csv",
                                                        addDbId = TRUE
                                                      ),
                                                      count_table = executionSettings$cohortCountTable,
                                                      min_exposure_size = executionSettings$minExposureSize,
                                                      table = executionSettings$cosineSimStratifiedTable,
                                                      results_database_schema = executionSettings$resultsDatabaseSchema)

  sql <- "SELECT * FROM @cdm_database_schema.cdm_source"
  DatabaseConnector::renderTranslateQueryApplyBatched(executionSettings$connection,
                                                      sql,
                                                      fun = exportResultsFun,
                                                      args = list(
                                                        csvFilename = "cdm_source_info.csv",
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
