skip_on_cran()

source("setup-synthetic-data.R")
createSyntheticTestDb("test_db/test_cse.db", tablePrefix = "test_")

testConnectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "duckdb",
  server = "test_db/test_cse.db"
)

# ---------------------------------------------------------------------------
# Helper: build a Plumber router from the package api.R with a real qns
# Does NOT call pr_run, so it's safe to use in tests.
# ---------------------------------------------------------------------------
buildTestRouter <- function(tablePrefix = "test_") {
  qns <- createResultsQueryNamespace(testConnectionDetails, "main", tablePrefix = tablePrefix)
  apiPath <- system.file("plumber", "api.R", package = "ComparatorSelectionExplorer")
  apiEnv <- new.env(parent = globalenv())
  apiEnv$qns <- qns
  apiEnv$tablePrefix <- tablePrefix
  list(
    pr  = plumber::Plumber$new(apiPath, envir = apiEnv),
    qns = qns
  )
}

# ---------------------------------------------------------------------------
# Router construction
# ---------------------------------------------------------------------------

test_that("Plumber$new with envir succeeds and exposes qns in router environment", {
  skip_if_not_installed("plumber")
  built <- buildTestRouter()
  pr <- built$pr

  expect_s3_class(pr, "Plumber")
  expect_identical(pr$environment$qns, built$qns)
  expect_identical(pr$environment$tablePrefix, "test_")
})

test_that("Plumber router registers all expected routes", {
  skip_if_not_installed("plumber")
  pr <- buildTestRouter()$pr

  # Collect all PlumberEndpoint paths from the recursive route tree
  collectPaths <- function(x) {
    if (inherits(x, "PlumberEndpoint")) return(x$path)
    if (is.list(x)) return(unlist(lapply(x, collectPaths), use.names = FALSE))
    NULL
  }

  paths <- collectPaths(pr$routes)

  expect_true("/api/cohorts" %in% paths)
  expect_true("/api/cohorts/<id:dbl>/rankings" %in% paths)
  expect_true("/api/cohorts/<id1:dbl>/compare/<id2:dbl>" %in% paths)
})

test_that("health and databases routes are registered", {
  skip_if_not_installed("plumber")
  pr <- buildTestRouter()$pr

  # pr$routes has a known bug in plumber 1.2.2: addPath drops sibling routes
  # when a same-prefix route also has sub-routes (e.g. /api/cohorts and
  # /api/cohorts/<id:int>/rankings). Use pr$endpoints instead, which is correct.
  all_paths <- unlist(lapply(pr$endpoints, function(grp) {
    lapply(grp, function(ep) ep$path)
  }), use.names = FALSE)

  expect_true("/api/health" %in% all_paths)
  expect_true("/api/databases" %in% all_paths)
})

# ---------------------------------------------------------------------------
# Route handler logic (exercises the same query functions used in api.R)
# ---------------------------------------------------------------------------

test_that("getDatabaseSources returns a non-empty data frame via qns", {
  skip_if_not_installed("plumber")
  qns <- createResultsQueryNamespace(testConnectionDetails, "main", tablePrefix = "test_")
  result <- getDatabaseSources(qns)
  expect_s3_class(result, "data.frame")
  expect_gt(nrow(result), 0)
})

test_that("getCohortDefinitions returns cohorts with expected columns via qns", {
  skip_if_not_installed("plumber")
  qns <- createResultsQueryNamespace(testConnectionDetails, "main", tablePrefix = "test_")
  cohorts <- getCohortDefinitions(qns)
  expect_s3_class(cohorts, "data.frame")
  expect_true("cohortDefinitionId" %in% colnames(cohorts))
  expect_true("shortName" %in% colnames(cohorts))
  expect_gt(nrow(cohorts), 0)
})

test_that("getCohortSimilarityScores returns data with required columns via qns", {
  skip_if_not_installed("plumber")
  qns <- createResultsQueryNamespace(testConnectionDetails, "main", tablePrefix = "test_")
  raw <- getCohortSimilarityScores(qns, targetCohortId = 101)
  expect_s3_class(raw, "data.frame")
  expect_true(all(c("cosineSimilarity", "databaseId", "cohortDefinitionId2") %in% colnames(raw)))
})
