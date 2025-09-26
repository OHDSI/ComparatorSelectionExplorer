test_that("Execution", {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  addFakeAtcVocab(connection)
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
  rxNormDefinition <-
    CohortGenerator::createRxNormCohortTemplateDefinition(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      priorObservationPeriod = 365,
      nameSuffix = ""
    )

  cohortDefinitionSet <- cohortDefinitionSet |>
    CohortGenerator::addCohortTemplateDefintion(cohortTemplateDefintion = rxNormDefinition) |>
    CohortGenerator::addCohortSubsetDefinition(subsetDef)

  executionSettings <- createExecutionSettings(connectionDetails = connectionDetails,
                                               cohortDefinitionSet = cohortDefinitionSet,
                                               cdmDatabaseSchema = "main",
                                               resultsDatabaseSchema = "main",
                                               cohortTable = "cse_cohort")

  CohortGenerator::runCohortGeneration(
    executionSettings$connectionDetails,
    cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
    tempEmulationSchema = executionSettings$tempEmulationSchema,
    cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
    cohortTableNames = executionSettings$cohortTableNames,
    cohortDefinitionSet = executionSettings$cohortDefinitionSet,
    outputFolder = tempfile(),
    databaseId = executionSettings$databaseId,
    incremental = TRUE,
    incrementalFolder = tempfile()
  )


  unlink(executionSettings$exportZipFile)

  on.exit({
    unlink(executionSettings$exportZipFile)
  })

  checkmate::expect_class(executionSettings, "executionSettings")
  addFakeAtcVocab(executionSettings)
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
