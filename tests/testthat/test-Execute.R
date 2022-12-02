test_that("Execution", {

  executionSettings <- createExecutionSettings(connectionDetails = connectionDetails,
                                               databaseName = "eunomia",
                                               cdmDatabaseSchema = "main",
                                               resultsDatabaseSchema = "main",
                                               cohortTable = "cse_cohort")
  unlink(executionSettings$exportZipFile)
  on.exit(unlink(executionSettings$exportZipFile))

  checkmate::expect_class(executionSettings, "executionSettings")
  execute(executionSettings)
  checkmate::expect_file_exists(executionSettings$exportZipFile)

  resultsConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite",
                                                                         server = "test.sqlite")
  on.exit(unlink("test.sqlite"), add = TRUE)
  createResultsDataModel(resultsConnectionDetails, "main")
})
