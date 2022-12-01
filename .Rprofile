source("renv/activate.R")

# details of results database
# change if testing on redshift
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  user = Sys.getenv("phenotypeLibrarydbUser"),
  password = Sys.getenv("phenotypeLibrarydbPw"),
  server = paste0(Sys.getenv("phenotypeLibraryServer"), "/", Sys.getenv("phenotypeLibrarydb")),
  port = 5432
)

# params
# resultsSchema <- "reward_truven_ccae_v1676"
resultsSchema <- "comparator_selector"
similarityTable <- "oscsp_similarity_for_shiny"
covDataTable <- "oscsp_covdata_for_shiny"
