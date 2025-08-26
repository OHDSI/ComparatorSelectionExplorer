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
    DatabaseConnector::disconnect(executionSettings$connection)
  })

  checkmate::expect_class(executionSettings, "executionSettings")
  addFakeAtcVocab(executionSettings)
  createCohorts(executionSettings)

  sql <- "SELECT * FROM @cohort_def_table"
  cohortRefs <- DatabaseConnector::renderTranslateQuerySql(executionSettings$connection, sql, cohort_def_table = executionSettings$cohortDefinitionTable, snakeCaseToCamelCase = TRUE)

  expect_equal(nrow(cohortRefs), length(unique(cohortRefs$cohortDefinitionId)))
})


test_that("Indication subsetting works", {

  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet()
  # Cohort 4 is test AMI cohort
  expect_true(any(cohortDefinitionSet$cohortId == 4))

  amiIndicationSubsetDefinition <- CohortGenerator::createCohortSubsetDefinition(
    name = "Prior history of AMI",
    definitionId = 1,
    subsetOperators = list(
      CohortGenerator::createCohortSubset(
        name = "AMI",
        cohortIds = 4,
        cohortCombinationOperator = "all",
        negate = FALSE,
        windows = list(
          CohortGenerator::createSubsetCohortWindow(
            startDay = -365,
            endDay = 0,
            targetAnchor = "cohortStart",
            subsetAnchor = "cohortStart"
          )
        )
      )
    )
  )

  expect_error(
  {
    executionSettings <- createExecutionSettings(connectionDetails = connectionDetails,
                                                 generateCohortDefinitionSet = TRUE,
                                                 indicationCohortSubsetDefintions = list(amiIndicationSubsetDefinition),
                                                 cdmDatabaseSchema = "main",
                                                 resultsDatabaseSchema = "main",
                                                 cohortTable = "cse_cohort")
  }, regexp = "Indication subset definitions added but no cohort definition set provided")

  executionSettings <- createExecutionSettings(connectionDetails = connectionDetails,
                                               generateCohortDefinitionSet = TRUE,
                                               cohortDefinitionSet = cohortDefinitionSet,
                                               indicationCohortSubsetDefintions = list(amiIndicationSubsetDefinition),
                                               cdmDatabaseSchema = "main",
                                               resultsDatabaseSchema = "main",
                                               cohortTable = "cse_cohort")
  checkmate::expect_list(executionSettings$indicationCohortSubsetDefintions, types = "CohortSubsetDefinition")
  expect_true(length(executionSettings$indicationCohortSubsetDefintions) == 1)

  unlink(executionSettings$exportZipFile)
  unlink(executionSettings$incrementalFolder, recursive = TRUE)
  dir.create(executionSettings$incrementalFolder)

  on.exit({
    unlink(executionSettings$exportZipFile)
    unlink(executionSettings$incrementalFolder, recursive = TRUE)
    DatabaseConnector::disconnect(executionSettings$connection)
  })

  addFakeAtcVocab(executionSettings)
  executionSettings <- createCohorts(executionSettings)

  sql <- "SELECT * FROM @cohort_def_table"
  cohortRefs <- DatabaseConnector::renderTranslateQuerySql(executionSettings$connection, sql, cohort_def_table = executionSettings$cohortDefinitionTable, snakeCaseToCamelCase = TRUE)

  expect_equal(nrow(cohortRefs), length(unique(cohortRefs$cohortDefinitionId)))
  # AMI cohort should have generated
  expect_true(4 %in% cohortRefs$cohortDefinitionId)
  # Check indication subsets were added
  expect_true(executionSettings$cohortDefinitionSet |> dplyr::filter(.data$isSubset) |> dplyr::select("cohortId", "cohortName") |> dplyr::count() > 1)

  expect_true(all(executionSettings$cohortDefinitionSet$cohortId %in% cohortRefs$cohortDefinitionId))
})
