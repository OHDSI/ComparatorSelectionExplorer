# details of results database
# Created for data.ohdsi.org
# details of results database
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  user = Sys.getenv("phenotypeLibrarydbUser"),
  password = Sys.getenv("phenotypeLibrarydbPw"),
  server = paste0(Sys.getenv("phenotypeLibraryServer"), "/", Sys.getenv("phenotypeLibrarydb")),
  port = 5432
)

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = "test.sqlite")