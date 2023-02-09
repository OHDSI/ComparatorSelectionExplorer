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


connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                server = "reward.cterqq54xyuu.us-east-1.rds.amazonaws.com/rewardb_dev",
                                                                user = "reward_admin",
                                                                password = keyring::key_get("reward-db", user = "reward_admin"),
                                                                port = 5432)
resultsSchema <- "cse_022023"
tablePrefix <- "cse_"