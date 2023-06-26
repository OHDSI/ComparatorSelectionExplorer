test_that("Execution", {

  # Load cohort definition set
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(settingsFileName = "Cohorts.csv",
                                                                 jsonFolder = "cohorts",
                                                                 sqlFolder = "sql/sql_server")


  subsetDef <- CohortGenerator::createCohortSubsetDefinition("Test subset",
                                                             definitionId = 1,
                                                             identifierExpression = "targetId * 100 + definitionId",
                                                             subsetOperators = list(
                                                               CohortGenerator::createDemographicSubset(
                                                               ageMin = 18,
                                                               ageMax = 64
                                                              )
                                                             ))

  executionSettings <- createExecutionSettings(connectionDetails = connectionDetails,
                                               cohortDefinitionSet = cohortDefinitionSet,
                                               indicationCohortSubsetDefintions = list(subsetDef),
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
  execute(executionSettings)
  checkmate::expect_file_exists(executionSettings$exportZipFile)

  unlink("test.sqlite")
  resultsConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite",
                                                                         server = "test.sqlite")
  on.exit(unlink("test.sqlite"), add = TRUE)
  createResultsDataModel(resultsConnectionDetails, "main", tablePrefix = "cse_")

  uploadResults(connectionDetails = resultsConnectionDetails,
                databaseSchema = "main",
                zipFileName = executionSettings$exportZipFile,
                forceOverWriteOfSpecifications = FALSE,
                purgeSiteDataBeforeUploading = FALSE,
                tablePrefix = "cse_")
})


test_that("Execution", {

  # Load cohort definition set
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(settingsFileName = "Cohorts.csv",
                                                                 jsonFolder = "cohorts",
                                                                 sqlFolder = "sql/sql_server")


  subsetDef <- CohortGenerator::createCohortSubsetDefinition("Test subset",
                                                             definitionId = 1,
                                                             identifierExpression = "targetId * 100 + definitionId",
                                                             subsetOperators = list(
                                                               CohortGenerator::createDemographicSubset(
                                                               ageMin = 18,
                                                               ageMax = 64
                                                              )
                                                             ))

  executionSettings <- createExecutionSettings(connectionDetails = connectionDetails,
                                               cohortDefinitionSet = cohortDefinitionSet,
                                               indicationCohortSubsetDefintions = list(subsetDef),
                                               generateCohortDefinitionSet = TRUE,
                                               targetCohortIds = c(1118084, 1118084 * 100 + 1),
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

  execute(executionSettings)
  checkmate::expect_file_exists(executionSettings$exportZipFile)

  unlink("test2.sqlite")
  resultsConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite",
                                                                         server = "test2.sqlite")
  on.exit(unlink("test2.sqlite"), add = TRUE)
  createResultsDataModel(resultsConnectionDetails, "main", tablePrefix = "cse_")

  uploadResults(connectionDetails = resultsConnectionDetails,
                databaseSchema = "main",
                zipFileName = executionSettings$exportZipFile,
                forceOverWriteOfSpecifications = FALSE,
                purgeSiteDataBeforeUploading = FALSE,
                tablePrefix = "cse_")
})