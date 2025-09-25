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



addFakeAtcVocab <- function(executionSettings) {
  sql <-   sql <- "
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
  "
  DatabaseConnector::renderTranslateExecuteSql(executionSettings$connection, sql)
  invisible()
}
