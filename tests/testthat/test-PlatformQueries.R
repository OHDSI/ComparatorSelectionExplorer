# Removing oracle for now
sqlTestDbmsPlatforms <- list(
  "bigquery" = "bigquery",
  "postgres" = "postgresql",
  "redshift" = "redshift",
  "spark" = "spark",
  "tsql" = "sql server",
  "snowflake" = "snowflake",
  "duckdb" = "duckdb",
  "sqlite" = "sqlite",
  "hive" = "hive"
)
mockExecutionSettings <- createMockExecutionSettings(databaseName = "test",
                                                     databaseId = "test",
                                                     cdmDatabaseSchema = "ohdsi",
                                                     vocabularyDatabaseSchema = "ohdsi",
                                                     resultsDatabaseSchema = "cohort",
                                                     cohortTable = "cse_cohort",
                                                     tempEmulationSchema = "tempdb",
                                                     cohortDefinitionSet = CohortGenerator::getCohortDefinitionSet(settingsFileName = "Cohorts.csv", jsonFolder = "cohorts", sqlFolder = "sql/sql_server"),
                                                     cohortCountTable = "cse_cohort_count",
                                                     cohortDefinitionTable = "cse_cohort_definition",
                                                     covariateDefTable = "cse_covariate_ref",
                                                     covariateMeansTable = "cse_covariate_means",
                                                     cosineSimStratifiedTable = "cse_cosine_sim",
                                                     conditionConceptCountsTable = "cse_condition_concept_counts",
                                                     cohortPersonTimeTable = "cse_cohort_person_time",
                                                     exportPreStudyDiagnostics = TRUE,
                                                     minExposureSize = 1000)


test_that("Platform test queries for cosine similarity", {
  skip_if_not(reticulate::py_module_available("sqlglot"))
  sqlglot <- reticulate::import("sqlglot")

  parseQuery <- function(sql, dbms) {
    tryCatch(
      sqlglot$parse(sql, dbms),
      error = function(err) {
        stop(paste("\n**** Error testing on platform", dbms, "******\n\n", sql, "\n\n", err))
      }
    )
  }

  for (dbms in names(sqlTestDbmsPlatforms)) {
    sql <- .getFeaturesSql(mockExecutionSettings, dbms = sqlTestDbmsPlatforms[[dbms]])
    expect_silent(parseQuery(sql, dbms))
    sql <- .getCosineSimilaritySql(mockExecutionSettings, dbms = sqlTestDbmsPlatforms[[dbms]])
    expect_silent(parseQuery(sql, dbms))
    sql <- .getPreStudyDiagnostics(mockExecutionSettings, dbms = sqlTestDbmsPlatforms[[dbms]])
    expect_silent(parseQuery(sql, dbms))
  }
})
