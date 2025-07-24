#' Generate cohort overlap statistics
#' @description generate Jaccard index, number of patients entering cohort 1 before 2, number of patients entering cohort 2 before 1, and number of patients entering cohorts 1 & 2 on the same date
#' @inheritParams execute
#' @export
generateOverlapStatistics <- function(executionSettings = NULL, ...) {
  if (is.null(executionSettings) || missing(executionSettings)) {
    executionSettings <- createExecutionSettings(...)
  }

  ParallelLogger::logInfo("Generating cohort overlap statistics")
  sql <- SqlRender::loadRenderTranslateSql(
    "CohortOverlap.sql",
    packageName = utils::packageName(),
    dbms = DatabaseConnector::dbms(executionSettings$connection),
    cohort_counts = executionSettings$cohortCountTable,
    cohort = executionSettings$cohortTableNames$cohortTable,
    results_database_schema = executionSettings$resultsDatabaseSchema,
    tempEmulationSchema = executionSettings$tempEmulationSchema)

  DatabaseConnector::executeSql(executionSettings$connection, sql)

  executionSettings$cohortOverlapExecuted <- TRUE

  invisible(executionSettings)
}
