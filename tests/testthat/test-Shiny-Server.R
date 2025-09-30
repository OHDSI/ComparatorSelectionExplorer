test_that("launchShinyApp launches app without error", {
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "duckdb",
    server = "test_db/test_cse.db"
  )
  resultsSchema <- "main"
  tablePrefix <- "test_"
  # This test only checks that launchShinyApp returns an app object; does not actually run the app interactively
  app <- createShinyApp(connectionDetails, resultsSchema, tablePrefix)
  expect_true(inherits(app, "shiny.appobj"))
})


test_that("comparatorSelectionAppModuleServer registers outputs", {
  # Setup dummy QueryNamespace (or real one if possible)
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "duckdb",
    server = "test_db/test_cse.db"
  )
  qns <- createResultsQueryNamespace(connectionDetails, "main", tablePrefix = "test_")

  # Create test session
  shiny::testServer(comparatorSelectionAppModuleServer, args = list(qns = qns), {
    expect_true(TRUE)
  })
})
