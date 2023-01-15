test_that("Execution", {

  # Load cohort definition set
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(settingsFileName = "Cohorts.csv",
                                                                 jsonFolder = "cohorts",
                                                                 sqlFolder = "sql/sql_server")

  executionSettings <- createExecutionSettings(connectionDetails = connectionDetails,
                                               cohortDefinitionSet = cohortDefinitionSet,
                                               generateCohortDefinitionSet = TRUE,
                                               cdmDatabaseSchema = "main",
                                               resultsDatabaseSchema = "main",
                                               cohortTable = "cse_cohort", )
  unlink(executionSettings$exportZipFile)
  on.exit(unlink(executionSettings$exportZipFile))

  checkmate::expect_class(executionSettings, "executionSettings")
  execute(executionSettings)
  checkmate::expect_file_exists(executionSettings$exportZipFile)

  unlink("test.sqlite")
  resultsConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite",
                                                                         server = "test.sqlite")
  on.exit(unlink("test.sqlite"), add = TRUE)
  createResultsDataModel(resultsConnectionDetails, "main")

  uploadResults(connectionDetails = resultsConnectionDetails,
                databaseSchema = "main",
                zipFileName = executionSettings$exportZipFile,
                forceOverWriteOfSpecifications = FALSE,
                purgeSiteDataBeforeUploading = FALSE,
                tablePrefix = "")
})
