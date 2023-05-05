# details of results database
# Created for data.ohdsi.org
# details of results database

JDBC_PATH <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER", "./.drivers")

if (!dir.exists(JDBC_PATH)) {
  dir.create(JDBC_PATH)
}

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  user = Sys.getenv("phenotypeLibrarydbUser"),
  password = Sys.getenv("phenotypeLibrarydbPw"),
  server = paste0(Sys.getenv("phenotypeLibraryServer"), "/", Sys.getenv("phenotypeLibrarydb")),
  port = 5432
)


if (connectionDetails$dbms != "sqlite" &&
  !any(grepl(connectionDetails$dbms, list.files(JDBC_PATH)))) {
  DatabaseConnector::downloadJdbcDrivers(connectionDetails$dbms)
}

resultsSchema <- "cse_042023"
tablePrefix <- "cse_"


