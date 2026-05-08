source("setup-synthetic-data.R")
createSyntheticTestDb("test_db/test_cse.db", tablePrefix = "test_")

test_that("launchShinyApp launches app without error", {
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "duckdb",
    server = "test_db/test_cse.db"
  )
  resultsSchema <- "main"
  tablePrefix <- "test_"
  app <- createShinyApp(connectionDetails, resultsSchema, tablePrefix)
  expect_true(inherits(app, "shiny.appobj"))
})

test_that("comparatorSelectionAppModuleServer registers outputs", {
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "duckdb",
    server = "test_db/test_cse.db"
  )
  qns <- createResultsQueryNamespace(connectionDetails, "main", tablePrefix = "test_")

  shiny::testServer(comparatorSelectionAppModuleServer, args = list(qns = qns), {
    expect_true(TRUE)
  })
})
