connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  server = paste0(Sys.getenv("phenotypeLibraryServer"),"/", Sys.getenv("phenotypeLibrarydb")),
  port = 5432,
  user = Sys.getenv("phenotypeLibrarydbUser"),
  password = Sys.getenv("phenotypeLibrarydbPw")
)

resultsSchema <- "comparator_selector"
tablePrefix <- "cse_202507_"
ComparatorSelectionExplorer::launchShinyApp(connectionDetails, resultsSchema, tablePrefix)