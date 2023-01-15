# details of results database
# Created for data.ohdsi.org
# connectionDetails <- DatabaseConnector::createConnectionDetails(
#   dbms = "postgresql",
#   user = Sys.getenv("phenotypeLibrarydbUser"),
#   password = Sys.getenv("phenotypeLibrarydbPw"),
#   server = paste0(Sys.getenv("phenotypeLibraryServer"), "/", Sys.getenv("phenotypeLibrarydb")),
#   port = 5432
# )
#
# # params
# # resultsSchema <- "reward_truven_ccae_v1676"
# resultsSchema <- "comparator_selector"
similarityTable <- "cosine_similarity"
covDataTable <- "covariate_mean"
databaseTable <- "cdm_source_info"

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = "test.sqlite")