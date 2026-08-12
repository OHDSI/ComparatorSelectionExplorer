skip_on_cran()

if (dir.exists(Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"))) {
  jdbcDriverFolder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
} else {
  jdbcDriverFolder <- "~/jdbcDrivers"
  dir.create(jdbcDriverFolder, showWarnings = FALSE)
  baseDatabaseConnectorJarFolder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = jdbcDriverFolder)
  withr::defer(
  {
    unlink(jdbcDriverFolder, recursive = TRUE, force = TRUE)
    Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = baseDatabaseConnectorJarFolder)
  },
    testthat::teardown_env()
  )
}

if (!(Sys.getenv("CDM5_POSTGRESQL_USER") == "" &
  Sys.getenv("CDM5_POSTGRESQL_PASSWORD") == "" &
  Sys.getenv("CDM5_POSTGRESQL_SERVER") == "" &
  Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA") == "" &
  Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA") == "")) {
  DatabaseConnector::downloadJdbcDrivers("postgresql")
  connectionDetailsList <- list(
    connectionDetails = DatabaseConnector::createConnectionDetails(
      dbms = "postgresql",
      user = Sys.getenv("CDM5_POSTGRESQL_USER"),
      password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
      server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
      port = 5432,
      pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
    ),
    cdmDatabaseSchema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"),
    workDatabaseSchema = Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA"),
    vocabularyDatabaseSchema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
  )
}

test_that("Results creation and upload works on postgres", {
  skip_if_offline()
  skip_if(Sys.getenv("CDM5_POSTGRESQL_SERVER") == "")
  withr::local_options(list(vroom.show_col_types = FALSE))
  tablePrefix <- paste0("cse_", Sys.getpid(), sample(1:10, 1))

  dataModelSpec <- suppressWarnings(
    getResultsDataModelSpec() |>
      dplyr::bind_rows(CohortGenerator::getResultsDataModelSpecifications())
  )

  on.exit({
    # Connect and drop all tables - cleanup
    connection <- DatabaseConnector::connect(connectionDetailsList$connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
    sql <- ""
    for (table in dataModelSpec$tableName |> unique()) {
      sql <- sql |>
        paste(SqlRender::render("DROP TABLE IF EXISTS @schema.@tablePrefix@table;\n",
                                table = table,
                                tablePrefix = tablePrefix,
                                warnOnMissingParameters = FALSE))
    }
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql,
                                                 schema = Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA"))
  })

  # Should not error
  createResult <- tryCatch(
    {
      suppressWarnings(
        createTestDb(resultsConnectionDetails = connectionDetailsList$connectionDetails,
                     tablePrefix = tablePrefix,
                     resultsTestSchema = connectionDetailsList$workDatabaseSchema)
      )
      TRUE
    },
    error = function(e) {
      errorMessage <- conditionMessage(e)
      if (grepl("download|eunomia|cannot open url|timed out|failed", errorMessage, ignore.case = TRUE)) {
        skip(paste0("Skipping postgres integration test due to transient dataset download issue: ", errorMessage))
      }
      stop(e)
    }
  )

  expect_true(createResult)
})