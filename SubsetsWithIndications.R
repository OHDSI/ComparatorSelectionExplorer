library(CohortGenerator)
library(ROhdsiWebApi)

# Load Excel sheet, filtering by TA (NSEP)
tciTriplets <- openxlsx::read.xlsx(
  xlsxFile = "asd_004_input_specification.xlsx",
  sheet = "TCI",
  startRow = 2,
  cols = 1:8
  ) |>
  dplyr::filter(TA == "NSEP")

# Creates a vector Target_cohort_id and Indication_cohort_id
atlasCohorts <- c(tciTriplets$Target_cohort_id,
                  tciTriplets$Indication_cohort_id) |>
                  as.numeric() |>
                  unique()

# Removes NA values from vector
atlasCohorts <- atlasCohorts[!is.na(atlasCohorts)]

#Establish a connection to the ATLAS WebApi
webApiUrl <- "https://epi.jnj.com:8443/WebAPI"
ROhdsiWebApi::authorizeWebApi(
  baseUrl = webApiUrl,
  authMethod = "windows",
)

# Create cohorts using
cohortDefinitionSet <- exportCohortDefinitionSet(baseUrl = webApiUrl,
                                                 cohortIds = atlasCohorts)
subsetDefinitionId <- 0

for (i in unique(tciTriplets$Indication_cohort_id)){
  if (is.na(i)){
    next
  }
  targetCohortIds <- dplyr::filter(tciTriplets, Indication_cohort_id == i) |>
    dplyr::pull(Target_cohort_id) |>
    unique()

  subsetDefinitionName <- dplyr::filter(tciTriplets, Indication_cohort_id == i) |>
    dplyr::pull(indication_cohort_name)


  subsetDefinitionId <- subsetDefinitionId + 1

  cohortDefinitionSet <- CohortGenerator::addIndicationSubsetDefinition (cohortDefinitionSet,
                                                  targetCohortIds,
                                                  i,
                                                  subsetDefinitionId,
                                                  subsetDefinitionName[[1]]
  )



}


# Get the Eunomia connection details
connectionDetails <- Eunomia::getEunomiaConnectionDetails()

# First get the cohort table names to use for this generation task
cohortTableNames <- getCohortTableNames(cohortTable = "cg_example")

# Next create the tables on the database
createCohortTables(
  connectionDetails = connectionDetails,
  cohortTableNames = cohortTableNames,
  cohortDatabaseSchema = "main"
)

# Generate the cohort set
cohortsGenerated <- generateCohortSet(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTableNames = cohortTableNames,
  cohortDefinitionSet = cohortDefinitionSet
)

