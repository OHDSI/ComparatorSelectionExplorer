test_that("Execution", {

  executionSettings <- createExecutionSettings(connectionDetails = connectionDetails,
                                               databaseId = "eunomia",
                                               cdmDatabaseSchema = "main",
                                               resultsDatabaseSchema = "main",
                                               cohortTable = "cse_cohort",
                                               exportZipFile = "exportZip.zip")

  checkmate::expect_class(executionSettings, "executionSettings")
  execute(executionSettings)

})
