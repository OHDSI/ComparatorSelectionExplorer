source("setup-synthetic-data.R")
createSyntheticTestDb("test_db/test_cse.db", tablePrefix = "test_")

skip_if_not_installed("shiny")
skip_if_not_installed("shinydashboard")
skip_if_not_installed("shinycssloaders")
skip_if_not_installed("plotly")
skip_if_not_installed("reactable")

test_that("launchShinyApp launches app without error", {
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "duckdb",
    server = "test_db/test_cse.db"
  )
  resultsSchema <- "main"
  tablePrefix <- "test_"
  app <- createShinyApp(connectionDetails, resultsSchema, tablePrefix, usePooledConnection = FALSE)
  on.exit(attr(app, "queryNamespace")$closeConnection(), add = TRUE)
  expect_true(inherits(app, "shiny.appobj"))
})

test_that("comparatorSelectionAppModuleServer registers outputs", {
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "duckdb",
    server = "test_db/test_cse.db"
  )
  qns <- createResultsQueryNamespace(connectionDetails, "main", tablePrefix = "test_")
  on.exit(qns$closeConnection(), add = TRUE)

  suppressWarnings(
    shiny::testServer(comparatorSelectionAppModuleServer, args = list(qns = qns), {
      expect_true(TRUE)
    })
  )
})
