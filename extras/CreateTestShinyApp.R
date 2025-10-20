devtools::load_all()
unlink("test_cse.db", force = TRUE, recursive = TRUE)
resultsConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "duckdb",

createTestShinyData(resultsConnectionDetails, "test_")
#devtools::load_all(); launchShinyApp(resultsConnectionDetails, "main", "test_")
# qns <- createResultsQueryNamespace(resultsConnectionDetails, "main", tablePrefix = "test_")