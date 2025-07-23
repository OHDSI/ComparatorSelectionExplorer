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


  purrr::walk(executionSettings$indicationCohortSubsetDefintions, function(subsetDef) {
    executionSettings$cohortDefinitionSet <<- executionSettings$cohortDefinitionSet |>
      CohortGenerator::addCohortSubsetDefinition(subsetDef)
  })

  CohortGenerator::generateCohortSet(connection = executionSettings$connection,
                                     cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
                                     tempEmulationSchema = executionSettings$tempEmulationSchema,
                                     cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
                                     cohortTableNames = executionSettings$cohortTableNames,
                                     cohortDefinitionSet = executionSettings$cohortDefinitionSet,
                                     stopOnError = TRUE,
                                     incremental = TRUE,
                                     incrementalFolder = executionSettings$incrementalFolder)

  # Insert cohort definition table
  cohortRef <-
    cohrtRef |> dplyr::bind_rows(
      executionSettings$cohortDefinitionSet |>
        dplyr::select("cohortId", "cohortName", "subsetParent") |>
        dplyr::mutate(atcFlag = -1,
                      conceptId = -1,
                      shortName = .data$cohortName) |>
        dplyr::rename("cohortDefinitionName" = "cohortName",
                      "cohortDefinitionId" = "cohortId")
    )

  colnames(cohortRef) <- toupper(SqlRender::camelCaseToSnakeCase(colnames(cohortRef)))
  DatabaseConnector::insertTable(connection = executionSettings$connection,
                                 data = cohortRef,
                                 tableName = executionSettings$cohortDefinitionTable,
                                 databaseSchema = executionSettings$resultsDatabaseSchema,
                                 camelCaseToSnakeCase = FALSE,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = FALSE)


  executionSettings$cohortsGenerated <- TRUE
  invisible(executionSettings)
}
