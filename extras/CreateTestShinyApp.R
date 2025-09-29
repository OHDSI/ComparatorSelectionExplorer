library(ComparatorSelectionExplorer)

createTestShinyData <- function(resultsConnectionDetails) {
  devtools::load_all()
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  addFakeAtcVocab(connection)


  rxNormDefinition <-
    CohortGenerator::createRxNormCohortTemplateDefinition(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      priorObservationPeriod = 365,
      nameSuffix = ""
    )

  atcDefinition <-
    CohortGenerator::createAtcCohortTemplateDefinition(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      priorObservationPeriod = 365,
      nameSuffix = ""
    )

  cohortDefinitionSet <-
    CohortGenerator::addCohortTemplateDefintion(cohortTemplateDefintion = rxNormDefinition) |>
    CohortGenerator::addCohortTemplateDefintion(cohortTemplateDefintion = atcDefinition)

  executionSettings <- createExecutionSettings(connection = connection,
                                               cohortDefinitionSet = cohortDefinitionSet,
                                               cdmDatabaseSchema = "main",
                                               resultsDatabaseSchema = "main",
                                               cohortTable = "cse_cohort")

  cgResFolder <- tempfile()
  CohortGenerator::runCohortGeneration(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
    tempEmulationSchema = executionSettings$tempEmulationSchema,
    cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
    cohortTableNames = executionSettings$cohortTableNames,
    cohortDefinitionSet = executionSettings$cohortDefinitionSet,
    outputFolder = cgResFolder,
    databaseId = executionSettings$databaseId,
    incremental = TRUE,
    incrementalFolder = tempfile()
  )

  CohortGenerator::createResultsDataModel(resultsConnectionDetails, "main", tablePrefix = "cse_")
  CohortGenerator::uploadResults(resultsConnectionDetails, "main",
                                 tablePrefix = "cse_",
                                 resultsFolder = cgResFolder,
                                 purgeSiteDataBeforeUploading = FALSE)

  unlink(executionSettings$exportZipFile)

  on.exit({
    unlink(executionSettings$exportZipFile)
  })

  execute(executionSettings)
  unlink("test.sqlite")

  on.exit(unlink("test.sqlite"), add = TRUE)
  createResultsDataModel(resultsConnectionDetails, "main", tablePrefix = "cse_")

  uploadResults(connectionDetails = resultsConnectionDetails,
                databaseSchema = "main",
                zipFileName = executionSettings$exportZipFile,
                forceOverWriteOfSpecifications = FALSE,
                purgeSiteDataBeforeUploading = FALSE,
                tablePrefix = "cse_")
}

unlink("test_cse.db")
resultsConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite",
                                                                       server = "./test_cse.db")
createTestShinyData(resultsConnectionDetails)
launchShinyApp(resultsConnectionDetails, "main", "cse_")