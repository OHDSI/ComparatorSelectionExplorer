.getPreStudyDiagnostics <- function(executionSettings, dbms = DatabaseConnector::dbms(executionSettings$connection)) {
  sql <- SqlRender::loadRenderTranslateSql("PreStudyDiagnostics.sql",
                                           packageName = utils::packageName(),
                                           dbms = dbms,
                                           cohort = executionSettings$cohortTableNames$cohortTable,
                                           cdm_database_schema = executionSettings$cdmDatabaseSchema,
                                           results_database_schema = executionSettings$resultsDatabaseSchema,
                                           condition_concept_counts = executionSettings$conditionConceptCountsTable,
                                           cohort_person_time = executionSettings$cohortPersonTimeTable,
                                           cohort_database_schema = executionSettings$cohortDatabaseSchema,
                                           tempEmulationSchema = executionSettings$tempEmulationSchema) |>
    as.character()

  return(sql)
}


#' Generate similarity scores
#' @description
#' create cosine similarity scores
#' @inheritParams
#' execute
#' @export
executePreStudyDiagnostics <- function(executionSettings = NULL, ...) {
  if (is.null(executionSettings) || missing(executionSettings)) {
    executionSettings <- createExecutionSettings(...)
  }

  ParallelLogger::logInfo("Executing time at risk and condition concept counts")
  sql <- .getPreStudyDiagnostics(executionSettings)

  DatabaseConnector::executeSql(executionSettings$connection, sql)

  executionSettings$preStudyDiagnosticsExecuted <- TRUE
  invisible(executionSettings)
}