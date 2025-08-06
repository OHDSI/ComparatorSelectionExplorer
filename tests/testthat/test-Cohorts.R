test_that("No dupes in ref table", {
  executionSettings <- createExecutionSettings(connectionDetails = connectionDetails,
                                               generateCohortDefinitionSet = TRUE,
                                               cdmDatabaseSchema = "main",
                                               resultsDatabaseSchema = "main",
                                               cohortTable = "cse_cohort")
  unlink(executionSettings$exportZipFile)
  unlink(executionSettings$incrementalFolder, recursive = TRUE)
  dir.create(executionSettings$incrementalFolder)

  on.exit({
    unlink(executionSettings$exportZipFile)
    unlink(executionSettings$incrementalFolder, recursive = TRUE)
  })

  checkmate::expect_class(executionSettings, "executionSettings")
  addFakeAtcVocab(executionSettings)
  createCohorts(executionSettings)

  sql <- "SELECT * FROM @cohort_def_table"
  cohortRefs <- DatabaseConnector::renderTranslateQuerySql(executionSettings$connection, sql, cohort_def_table = executionSettings$cohortDefinitionTable, snakeCaseToCamelCase = TRUE)

  expect_equal(nrow(cohortRefs), length(unique(cohortRefs$cohortDefinitionId)))
})