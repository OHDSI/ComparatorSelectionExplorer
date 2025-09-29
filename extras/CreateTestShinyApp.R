devtools::load_all()
unlink("tests/testthat/test_db/test_cse.db", force = TRUE, recursive = TRUE)
resultsConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "duckdb",
                                                                       server = "tests/testthat/test_db/test_cse.db")
createTestShinyData(resultsConnectionDetails, "test_")
#launchShinyApp(resultsConnectionDetails, "main", "test_")
# qns <- createResultsQueryNamespace(resultsConnectionDetails, "main", tablePrefix = "test_")