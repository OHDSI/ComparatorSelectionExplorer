# Tests for uploading on postgres
library(testthat)
library(CohortGenerator)

testPlatform <- function(dbmsDetails) {
  cohortTableNames <- getCohortTableNames(cohortTable = dbmsDetails$cohortTable)
  platformOutputFolder <- file.path(tempfile(), dbmsDetails$connectionDetails$dbms)



  # Load cohort definition set
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(settingsFileName = "Cohorts.csv",
                                                                 jsonFolder = "cohorts",
                                                                 sqlFolder = "sql/sql_server")

  # Large vectors makes cosine similarity calculation slow
  if (getOption("cseTestUseFullCohorts", default = FALSE)) {
    connection <- DatabaseConnector::connect(dbmsDetails$connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
    rxNormDefinition <-
      CohortGenerator::createRxNormCohortTemplateDefinition(
        connection = connection,
        cdmDatabaseSchema = dbmsDetails$cdmDatabaseSchema,
        cohortDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
        priorObservationPeriod = 365,
        nameSuffix = ""
      )

    cohortDefinitionSet <- cohortDefinitionSet |>
      CohortGenerator::addCohortTemplateDefintion(cohortTemplateDefintion = rxNormDefinition)
  }

  executionSettings <- createExecutionSettings(connectionDetails = dbmsDetails$connectionDetails,
                                               cohortDefinitionSet = cohortDefinitionSet,
                                               cdmDatabaseSchema = dbmsDetails$cdmDatabaseSchema,
                                               resultsDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
                                               # NOTE Just tests the sql and export - so using a small count here
                                               targetCohortIds = cohortDefinitionSet$targetCohortId[1:5,],
                                               cohortTable = "cse_cohort")

  # NOTE - this should use cached cohorts to speed up computation time
  CohortGenerator::runCohortGeneration(
    connectionDetails = executionSettings$connectionDetails,
    cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
    tempEmulationSchema = executionSettings$tempEmulationSchema,
    cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
    cohortTableNames = executionSettings$cohortTableNames,
    cohortDefinitionSet = executionSettings$cohortDefinitionSet,
    outputFolder = platformOutputFolder,
    databaseId = executionSettings$databaseId,
    incremental = TRUE,
    incrementalFolder = tempfile()
  )

 unlink(executionSettings$exportZipFile)

  on.exit({
    unlink(executionSettings$exportZipFile)
  })

  checkmate::expect_class(executionSettings, "executionSettings")
  execute(executionSettings)
  checkmate::expect_file_exists(executionSettings$exportZipFile)
}


# This file contains platform specific tests
test_that("platform specific create cohorts with stats, Incremental, get results", {
  skip_on_cran()
  # Note that these tests are designed to be quick and just test the platform in a general way
  # Sqlite completes the bulk of the packages testing
  for (dbmsPlatform in dbmsPlatforms) {
    dbmsDetails <- getPlatformConnectionDetails(dbmsPlatform)
    if (is.null(dbmsDetails)) {
      print(paste("No platform details available for", dbmsPlatform))
    } else {
      print(paste("Testing", dbmsPlatform))
      testPlatform(dbmsDetails)
    }
  }
})
