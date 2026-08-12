getPlatformConnectionDetails <- function(dbmsPlatform) {
  # Get drivers for test platform
  if (dir.exists(Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"))) {
    jdbcDriverFolder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  } else {
    jdbcDriverFolder <- "~/.jdbcDrivers"
    dir.create(jdbcDriverFolder, showWarnings = FALSE)
  }

  options("sqlRenderTempEmulationSchema" = NULL)
  if (dbmsPlatform == "sqlite") {
    connectionDetails <- Eunomia::getEunomiaConnectionDetails()
    cdmDatabaseSchema <- "main"
    vocabularyDatabaseSchema <- "main"
    cohortDatabaseSchema <- "main"
    options("sqlRenderTempEmulationSchema" = NULL)
    cohortTable <- "cohort"
  } else {
    if (dbmsPlatform == "bigquery") {
      # To avoid rate limit on BigQuery, only test on 1 OS:
      if (.Platform$OS.type == "windows") {
        bqKeyFile <- tempfile(fileext = ".json")
        writeLines(Sys.getenv("CDM_BIG_QUERY_KEY_FILE"), bqKeyFile)
        if (testthat::is_testing()) {
          withr::defer(unlink(bqKeyFile, force = TRUE), testthat::teardown_env())
        }
        bqConnectionString <- gsub(
          "<keyfile path>",
          normalizePath(bqKeyFile, winslash = "/"),
          Sys.getenv("CDM_BIG_QUERY_CONNECTION_STRING")
        )
        connectionDetails <- DatabaseConnector::createConnectionDetails(
          dbms = dbmsPlatform,
          user = "",
          password = "",
          connectionString = !!bqConnectionString,
          pathToDriver = jdbcDriverFolder
        )
        cdmDatabaseSchema <- Sys.getenv("CDM_BIG_QUERY_CDM_SCHEMA")
        vocabularyDatabaseSchema <- Sys.getenv("CDM_BIG_QUERY_CDM_SCHEMA")
        cohortDatabaseSchema <- Sys.getenv("CDM_BIG_QUERY_OHDSI_SCHEMA")
        options(sqlRenderTempEmulationSchema = Sys.getenv("CDM_BIG_QUERY_OHDSI_SCHEMA"))
      } else {
        return(NULL)
      }
    } else if (dbmsPlatform == "oracle") {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM5_ORACLE_USER"),
        password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
        server = Sys.getenv("CDM5_ORACLE_SERVER"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA")
      options(sqlRenderTempEmulationSchema = Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA"))
    } else if (dbmsPlatform == "postgresql") {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
        password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
        server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA")
    } else if (dbmsPlatform == "redshift") {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM5_REDSHIFT_USER"),
        password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
        server = Sys.getenv("CDM5_REDSHIFT_SERVER"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_OHDSI_SCHEMA")
    } else if (dbmsPlatform == "snowflake") {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM_SNOWFLAKE_USER"),
        password = URLdecode(Sys.getenv("CDM_SNOWFLAKE_PASSWORD")),
        connectionString = Sys.getenv("CDM_SNOWFLAKE_CONNECTION_STRING"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM_SNOWFLAKE_CDM53_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM_SNOWFLAKE_CDM53_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM_SNOWFLAKE_OHDSI_SCHEMA")
      options(sqlRenderTempEmulationSchema = Sys.getenv("CDM_SNOWFLAKE_OHDSI_SCHEMA"))
    } else if (dbmsPlatform == "spark") {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM5_SPARK_USER"),
        password = URLdecode(Sys.getenv("CDM5_SPARK_PASSWORD")),
        connectionString = Sys.getenv("CDM5_SPARK_CONNECTION_STRING"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM5_SPARK_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_SPARK_CDM_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM5_SPARK_OHDSI_SCHEMA")
      options(sqlRenderTempEmulationSchema = Sys.getenv("CDM5_SPARK_OHDSI_SCHEMA"))
    } else if (dbmsPlatform == "sql server") {
      connectionDetails <- createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM5_SQL_SERVER_USER"),
        password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
        server = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_OHDSI_SCHEMA")
    }

    # Add drivers
    DatabaseConnector::downloadJdbcDrivers(dbmsPlatform, pathToDriver = jdbcDriverFolder)
    # cached cohort table to save time - collisions can occur on first creation of cohorts
    cohortTable <- "cse_cohort_cache"
  }

  return(list(
    dbmsPlatform = dbmsPlatform,
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema
  ))
}


addFakeAtcVocab <- function(connection) {
  sql <- "
INSERT INTO concept (CONCEPT_ID, CONCEPT_NAME, DOMAIN_ID, VOCABULARY_ID, CONCEPT_CLASS_ID,
                     STANDARD_CONCEPT, CONCEPT_CODE, VALID_START_DATE, VALID_END_DATE)
SELECT
  21603991 as concept_id, 'Coxibs' as concept_name, 'Drug' as domain_id, 'ATC' as vocabulary_id,
  'ATC 4th' as concept_class_id, 'C' as  standard_concept, 'M01AH' as concept_code,
  '1970-01-01' as valid_start_date, '2099-12-31' as valid_end_date;

INSERT INTO concept_ancestor (ancestor_concept_id, descendant_concept_id, min_levels_of_separation,
                              max_levels_of_separation)
SELECT 21603991 as ancestor_concept_id, 1118084 as descendant_concept_id,
       1 as min_levels_of_separation,1  as max_levels_of_separation;

  -- ATC ancestor concept
INSERT INTO concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id,
                     standard_concept, concept_code, valid_start_date, valid_end_date)
SELECT
  300 AS concept_id, 'ATC Example' AS concept_name, 'Drug' AS domain_id, 'ATC' AS vocabulary_id,
  'ATC 2nd' AS concept_class_id, 'C' AS standard_concept, 'A01' AS concept_code,
  '1970-01-01' AS valid_start_date, '2099-12-31' AS valid_end_date
UNION
SELECT
  101 AS concept_id, 'Drug A' AS concept_name, 'Drug' AS domain_id, 'RxNorm' AS vocabulary_id,
  'ATC 3rd' AS concept_class_id, 'C' AS standard_concept, 'X01' AS concept_code,
  '1970-01-01' AS valid_start_date, '2099-12-31' AS valid_end_date
UNION
SELECT
  102 AS concept_id, 'Drug B' AS concept_name, 'Drug' AS domain_id, 'RxNorm' AS vocabulary_id,
  'ATC 5th' AS concept_class_id, 'C' AS standard_concept, 'X02' AS concept_code,
  '1970-01-01' AS valid_start_date, '2099-12-31' AS valid_end_date;

-- Both RxNorm drugs map to the same ATC ancestor
INSERT INTO concept_ancestor (ancestor_concept_id, descendant_concept_id, min_levels_of_separation,
                              max_levels_of_separation)
SELECT
  300 AS ancestor_concept_id, 101 AS descendant_concept_id, 1 AS min_levels_of_separation, 1 AS max_levels_of_separation
UNION ALL
SELECT
  300 AS ancestor_concept_id, 102 AS descendant_concept_id, 1 AS min_levels_of_separation, 1 AS max_levels_of_separation;


  "
  DatabaseConnector::renderTranslateExecuteSql(connection, sql)
  invisible()
}


createMockExecutionSettings <- function(connectionDetails = DatabaseConnector::createConnectionDetails("sqlite", server = ":memory:"),
                                        connection = NULL,
                                        databaseName = NULL,
                                        databaseId = NULL,
                                        cdmDatabaseSchema,
                                        vocabularyDatabaseSchema = cdmDatabaseSchema,
                                        resultsDatabaseSchema,
                                        cohortDatabaseSchema = resultsDatabaseSchema,
                                        cohortTable,
                                        tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                        cohortDefinitionSet = NULL,
                                        targetCohortIds = NULL,
                                        cohortCountTable = "cse_cohort_count",
                                        cohortDefinitionTable = "cse_cohort_definition",
                                        covariateDefTable = "cse_covariate_ref",
                                        covariateMeansTable = "cse_covariate_means",
                                        cosineSimStratifiedTable = "cse_cosine_sim",
                                        minExposureSize = 1000,
                                        logFileLocation = paste0("cse-execution-log-", cdmDatabaseSchema, ".txt"),
                                        exportDir = tempfile(),
                                        removeExportDir = TRUE,
                                        exportZipFile = file.path(normalizePath(getwd()), paste0("cse_results_", cdmDatabaseSchema, ".zip"))) {

  executionSettings <- list(connectionDetails = connectionDetails,
                            cdmDatabaseSchema = cdmDatabaseSchema,
                            databaseName = databaseName,
                            vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                            resultsDatabaseSchema = resultsDatabaseSchema,
                            cohortDatabaseSchema = cohortDatabaseSchema,
                            tempEmulationSchema = tempEmulationSchema,
                            exportZipFile = exportZipFile,
                            logFileLocation = logFileLocation,
                            cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable),
                            cohortCountTable = cohortCountTable,
                            cohortDefinitionTable = cohortDefinitionTable,
                            covariateDefTable = covariateDefTable,
                            covariateMeansTable = covariateMeansTable,
                            cosineSimStratifiedTable = cosineSimStratifiedTable,
                            minExposureSize = minExposureSize,
                            exportDir = exportDir,
                            removeExportDir = removeExportDir,
                            cohortDefinitionSet = cohortDefinitionSet,
                            targetCohortIds = targetCohortIds,
                            connection = connection)
  class(executionSettings) <- "executionSettings"

  executionSettings$databaseId <- databaseId
  if (is.null(executionSettings$databaseId)) {

    executionSettings$databaseId <- abs(digest::digest2int(paste("OHDSI", collapse = ""), seed = 999))
  }

  if (is.null(executionSettings$databaseName)) {
    executionSettings$databaseName <- "OHDSI"
  }

  return(executionSettings)
}


testPlatform <- function(dbmsDetails) {
  cohortTableNames <- getCohortTableNames(cohortTable = dbmsDetails$cohortTable)
  platformOutputFolder <- file.path(tempfile(), dbmsDetails$connectionDetails$dbms)


  # Load cohort definition set
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(settingsFileName = "Cohorts.csv",
                                                                 jsonFolder = "cohorts",
                                                                 sqlFolder = "sql/sql_server")

  # Large vectors makes cosine similarity calculation slow
  if (getOption("cseTestUseFullCohorts", default = FALSE)) {
    connection <- DatabaseConnector::connect(dbmsDetails$connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
    rxNormDefinition <-
      CohortGenerator::createRxNormCohortTemplateDefinition(
        connection = connection,
        cdmDatabaseSchema = dbmsDetails$cdmDatabaseSchema,
        cohortDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
        priorObservationPeriod = 365,
        nameSuffix = ""
      )

    cohortDefinitionSet <- cohortDefinitionSet |>
      CohortGenerator::addCohortTemplateDefintion(cohortTemplateDefintion = rxNormDefinition)
  }

  executionSettings <- createExecutionSettings(connectionDetails = dbmsDetails$connectionDetails,
                                               cohortDefinitionSet = cohortDefinitionSet,
                                               cdmDatabaseSchema = dbmsDetails$cdmDatabaseSchema,
                                               resultsDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
                                               # NOTE Just tests the sql and export - so using a small count here
                                               targetCohortIds = cohortDefinitionSet$targetCohortId[1:5,],
                                               cohortTable = "cse_cohort")

  # NOTE - this should use cached cohorts to speed up computation time
  CohortGenerator::runCohortGeneration(
    connectionDetails = executionSettings$connectionDetails,
    cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
    tempEmulationSchema = executionSettings$tempEmulationSchema,
    cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
    cohortTableNames = executionSettings$cohortTableNames,
    cohortDefinitionSet = executionSettings$cohortDefinitionSet,
    outputFolder = platformOutputFolder,
    databaseId = executionSettings$databaseId,
    incremental = TRUE
  )

  on.exit({
    unlink(executionSettings$exportZipFile)
  })

  checkmate::expect_class(executionSettings, "executionSettings")
  execute(executionSettings)
  checkmate::expect_file_exists(executionSettings$exportZipFile)
}

createTestDb <- function(resultsConnectionDetails, tablePrefix, resultsTestSchema = "main") {

  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  addFakeAtcVocab(connection)


  rxNormDefinition <-
    CohortGenerator::createRxNormCohortTemplateDefinition(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      priorObservationPeriod = 365,
      nameSuffix = ""
    )

  atcDefinition <-
    CohortGenerator::createAtcCohortTemplateDefinition(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      priorObservationPeriod = 365,
      nameSuffix = ""
    )

  cohortDefinitionSet <-
    CohortGenerator::addCohortTemplateDefintion(cohortTemplateDefintion = rxNormDefinition) |>
      CohortGenerator::addCohortTemplateDefintion(cohortTemplateDefintion = atcDefinition)

  executionSettings <- createExecutionSettings(connection = connection,
                                               cohortDefinitionSet = cohortDefinitionSet,
                                               cdmDatabaseSchema = "main",
                                               resultsDatabaseSchema = "main",
                                               cohortTable = "cse_cohort")

  cgResFolder <- tempfile()
  CohortGenerator::runCohortGeneration(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
    tempEmulationSchema = executionSettings$tempEmulationSchema,
    cohortDatabaseSchema = executionSettings$cohortDatabaseSchema,
    cohortTableNames = executionSettings$cohortTableNames,
    cohortDefinitionSet = executionSettings$cohortDefinitionSet,
    outputFolder = cgResFolder,
    databaseId = executionSettings$databaseId,
    incremental = TRUE
  )

  CohortGenerator::createResultsDataModel(resultsConnectionDetails, resultsTestSchema, tablePrefix = tablePrefix)
  CohortGenerator::uploadResults(resultsConnectionDetails,
                                 resultsTestSchema,
                                 tablePrefix = tablePrefix,
                                 resultsFolder = cgResFolder,
                                 purgeSiteDataBeforeUploading = FALSE)

  on.exit({
    unlink(executionSettings$exportZipFile)
  })

  execute(executionSettings)
  createResultsDataModel(resultsConnectionDetails, resultsTestSchema, tablePrefix = tablePrefix)

  uploadResults(connectionDetails = resultsConnectionDetails,
                databaseSchema = resultsTestSchema,
                zipFileName = executionSettings$exportZipFile,
                forceOverWriteOfSpecifications = FALSE,
                purgeSiteDataBeforeUploading = FALSE,
                tablePrefix = tablePrefix)
}
