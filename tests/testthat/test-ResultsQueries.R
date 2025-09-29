# Setup: create connection details and query namespace
resultsConnectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "duckdb",
  server = "test_db/test_cse.db"
)

qns <- createResultsQueryNamespace(
  resultsConnectionDetails,
  resultsSchema = "main",
  tablePrefix = "test_"
)

test_that("getCohortDefinitions returns a data.frame", {
  res <- getCohortDefinitions(qns)
  expect_true(inherits(res, "data.frame"))
  expect_true(all(c("cohortDefinitionId", "shortName", "isAtc") %in% colnames(res)))
})

test_that("getCohortTags returns tags for given cohortIds", {
  cohortIds <- c(1,2)
  res <- getCohortTags(qns, cohortIds)
  expect_true(inherits(res, "data.frame"))
  expect_true("tag" %in% colnames(res))
})

test_that("getDbDataSourcesTable returns expected structure", {
  res <- getDbDataSourcesTable(qns, reactableTable = FALSE)
  expect_true(inherits(res, "data.frame"))
  expect_true("cdmSourceAbbreviation" %in% colnames(res))
})

test_that("getCoOccurenceTableData returns expected columns", {
  res <- getCoOccurenceTableData(
    qns,
    databaseIds = c(280743270),
    prevInputHighMax = 0.9,
    prevInputHighMin = 0.8,
    prevInputLowMax = 0.5,
    prevInputLowMin = 0.3,
    cohortDefinitionId1 = 101,
    cohortDefinitionId2 = 202
  )
  expect_true(inherits(res, "data.frame"))
  expect_true("covariateId" %in% colnames(res))
})

test_that("getCohortDefinitionsTable returns expected columns", {
  res <- getCohortDefinitionsTable(qns, databaseId = 280743270, counts = TRUE)
  expect_true(inherits(res, "data.frame"))
  expect_true(all(c("cohortDefinitionId", "shortName", "numPersons", "databaseId") %in% colnames(res)))
})

test_that("getPairwiseCovariateData returns expected columns", {
  res <- getPairwiseCovariateData(qns, databaseId = 280743270, cohortDefinitionId1 = 101, cohortDefinitionId2 = 202)
  expect_true(inherits(res, "data.frame"))
  expect_true(all(c("covariateId", "mean1", "mean2", "stdDiff") %in% colnames(res)))
})

test_that("getCohortSimilarityScores returns expected columns", {
  res <- getCohortSimilarityScores(qns, targetCohortId = 101)
  expect_true(inherits(res, "data.frame"))
  expect_true("cosineSimilarity" %in% colnames(res))
})

test_that("getDatabaseSimilarityScores returns expected columns", {
  res <- getDatabaseSimilarityScores(qns, targetCohortId = 101, databaseIds = c(280743270, 280743270))
  expect_true(inherits(res, "data.frame"))
  expect_true("cosineSimilarity" %in% colnames(res))
})

test_that("getDbCosineSimilarityTable returns expected columns", {
  res <- getDbCosineSimilarityTable(qns, targetCohortId = 101, comparatorCohortId = 202, databaseId = 280743270)
  expect_true(inherits(res, "data.frame"))
  expect_true("cosineSimilarity" %in% colnames(res))
})

test_that("getDatabaseSources returns expected columns", {
  res <- getDatabaseSources(qns)
  expect_true(inherits(res, "data.frame"))
  expect_true("cdmSourceAbbreviation" %in% colnames(res))
})