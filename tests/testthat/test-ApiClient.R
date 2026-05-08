source("setup-synthetic-data.R")
createSyntheticTestDb("test_db/test_cse.db", tablePrefix = "test_")

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "duckdb",
  server = "test_db/test_cse.db"
)

qns <- createResultsQueryNamespace(connectionDetails, "main", tablePrefix = "test_")

# --- Test ranking pipeline (same logic as /api/cohorts/{id}/rankings handler) ---

test_that("Ranking pipeline produces correct structure", {
  raw <- getCohortSimilarityScores(qns, targetCohortId = 101)
  expect_true(inherits(raw, "data.frame"))
  expect_true("cosineSimilarity" %in% colnames(raw))
  expect_true(nrow(raw) > 0)

  filtered <- raw |>
    dplyr::arrange(.data$databaseId, .data$cdmSourceAbbreviation, dplyr::desc(.data$cosineSimilarity)) |>
    dplyr::group_by(.data$databaseId, .data$cdmSourceAbbreviation) |>
    dplyr::mutate(
      cdmSpecificRank = dplyr::row_number(),
      comparatorsInCdm = dplyr::n_distinct(.data$cohortDefinitionId2)
    ) |>
    dplyr::ungroup() |>
    dplyr::group_by(.data$cohortDefinitionId2) |>
    dplyr::filter(dplyr::n() >= 1) |>
    dplyr::ungroup()

  expect_true(nrow(filtered) > 0)

  ranked <- filtered |>
    dplyr::group_by(.data$cohortDefinitionId2, .data$shortName, .data$isAtc2) |>
    dplyr::summarise(
      nDatabases = dplyr::n(),
      avgSimilarity = mean(.data$cosineSimilarity),
      avgCdmRank = mean(.data$cdmSpecificRank),
      .groups = "drop"
    ) |>
    dplyr::arrange(dplyr::desc(.data$avgSimilarity)) |>
    dplyr::mutate(rank = dplyr::row_number())

  expect_true(all(c("rank", "cohortDefinitionId2", "shortName", "avgSimilarity", "nDatabases") %in% colnames(ranked)))
  expect_true(nrow(ranked) > 0)
})

# --- Test pairwise comparison (same logic as /api/cohorts/{id1}/compare/{id2}) ---

test_that("Pairwise comparison returns domain scores", {
  detail <- getDbCosineSimilarityTable(
    qns,
    targetCohortId = 101,
    comparatorCohortId = 202,
    databaseId = 280743270,
    returnReactable = FALSE
  )
  expect_true(inherits(detail, "data.frame"))
  expect_true("covariateType" %in% colnames(detail))
  expect_true(nrow(detail) > 0)
})

# --- Test cohort search logic ---

test_that("Cohort search logic filters correctly", {
  cohorts <- getCohortDefinitions(qns)
  expect_true(inherits(cohorts, "data.frame"))

  matched <- cohorts[grepl("Drug A", cohorts$shortName, ignore.case = TRUE), , drop = FALSE]
  expect_true(nrow(matched) >= 1)
})

# --- Test API client URL construction (without live server) ---

test_that("startComparatorApi errors with missing connection details", {
  expect_error(startComparatorApi(connectionDetails = NULL, resultsSchema = NULL), "ConnectionDetails")
})
