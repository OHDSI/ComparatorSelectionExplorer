createSyntheticTestDb <- function(dbPath = "test_db/test_cse.db", tablePrefix = "") {
  dbDir <- dirname(dbPath)
  if (!dir.exists(dbDir)) {
    dir.create(dbDir, recursive = TRUE)
  }
  unlink(dbPath)

  conn <- DatabaseConnector::connect(
    DatabaseConnector::createConnectionDetails(dbms = "duckdb", server = dbPath)
  )
  on.exit(DatabaseConnector::disconnect(conn), add = TRUE)

  p <- function(name) paste0(tablePrefix, name)

  DatabaseConnector::insertTable(conn, tableName = p("cg_cohort_definition"), data = data.frame(
    cohort_definition_id = c(101L, 102L, 201L, 202L, 301L, 302L),
    cohort_name = c("RxNorm - Drug A", "RxNorm - Drug B",
                    "ATC - Class X", "ATC - Class Y",
                    "Custom - Population Z", "Custom - Population W"),
    description = rep("", 6),
    json = rep("", 6),
    sql_command = rep("", 6),
    subset_parent = c(NA, NA, NA, NA, 101L, 102L),
    is_subset = c(0L, 0L, 0L, 0L, 1L, 1L),
    is_template = c(1L, 1L, 0L, 0L, 0L, 0L),
    subset_definition_id = c(NA, NA, NA, NA, 1L, 1L),
    stringsAsFactors = FALSE
  ), camelCaseToSnakeCase = FALSE)

  DatabaseConnector::insertTable(conn, tableName = p("cg_cohort_count"), data = data.frame(
    database_id = c(rep("280743270", 6), rep("280743271", 6)),
    cohort_id = rep(c(101L, 102L, 201L, 202L, 301L, 302L), 2),
    cohort_entries = c(50000L, 30000L, 10000L, 8000L, 2000L, 1500L,
                       45000L, 28000L, 9000L, 7500L, 1800L, 1200L),
    cohort_subjects = c(48000L, 29000L, 9500L, 7800L, 1900L, 1400L,
                        43000L, 27000L, 8500L, 7200L, 1700L, 1100L),
    stringsAsFactors = FALSE
  ), camelCaseToSnakeCase = FALSE)

  DatabaseConnector::insertTable(conn, tableName = p("cse_cohort_count"), data = data.frame(
    cohort_definition_id = c(101L, 102L, 201L, 202L, 301L, 302L, 101L, 102L, 201L, 202L, 301L, 302L),
    database_id = c(rep(280743270L, 6), rep(280743271L, 6)),
    num_persons = c(48000L, 29000L, 9500L, 7800L, 1900L, 1400L,
                    43000L, 27000L, 8500L, 7200L, 1700L, 1100L),
    stringsAsFactors = FALSE
  ), camelCaseToSnakeCase = FALSE)

  DatabaseConnector::insertTable(conn, tableName = p("cse_cdm_source_info"), data = data.frame(
    database_id = c(280743270L, 280743271L),
    cdm_source_abbreviation = c("CCAE", "MDCR"),
    cdm_holder = c("Optum", "CMS"),
    source_description = c("Commercial claims", "Medicare"),
    source_documentation_reference = c("", ""),
    cdm_etl_reference = c("", ""),
    source_release_date = as.Date(c("2024-01-01", "2024-01-01")),
    cdm_release_date = as.Date(c("2024-06-01", "2024-06-01")),
    cdm_version = c("v5.4", "v5.4"),
    vocabulary_version = c("v2024", "v2024"),
    stringsAsFactors = FALSE
  ), camelCaseToSnakeCase = FALSE)

  DatabaseConnector::insertTable(conn, tableName = p("cse_cohort_tag"), data = data.frame(
    cohort_definition_id = c(101L, 102L, 201L, 202L, 201L, 202L),
    tag = c("RxNorm", "RxNorm", "ATC", "ATC", "default", "default"),
    stringsAsFactors = FALSE
  ), camelCaseToSnakeCase = FALSE)

  DatabaseConnector::insertTable(conn, tableName = p("cse_covariate_definition"), data = data.frame(
    covariate_id = c(1001L, 1002L, 2001L, 2002L, 3001L, 3002L, 4001L, 4002L, 5001L, 5002L),
    covariate_name = c("Age group 18-30", "Age group 31-64",
                        "GI Bleed dx", "Chest pain dx",
                        "Diabetes", "Hypertension",
                        "Metformin use", "Statin use",
                        "Outpatient visit", "ED visit"),
    concept_id = c(0L, 0L, 4329847L, 314666L, 201820L, 316866L, 1503297L, 1559684L, 9202L, 9203L),
    time_at_risk_start = c(0L, 0L, -30L, -30L, -365L, -365L, -365L, -365L, -30L, -30L),
    time_at_risk_end = c(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L),
    covariate_type = c("Demographics", "Demographics",
                        "Presentation", "Presentation",
                        "Medical history", "Medical history",
                        "prior meds", "prior meds",
                        "visit context", "visit context"),
    stringsAsFactors = FALSE
  ), camelCaseToSnakeCase = FALSE)

  set.seed(42)
  db_ids <- c(280743270L, 280743271L)
  cohort_ids <- c(101L, 102L, 201L, 202L, 301L, 302L)
  covariate_ids <- c(1001L, 1002L, 2001L, 2002L, 3001L, 3002L, 4001L, 4002L, 5001L, 5002L)
  means_rows <- do.call(rbind, lapply(db_ids, function(dbid) {
    do.call(rbind, lapply(cohort_ids, function(cid) {
      data.frame(
        database_id = dbid,
        cohort_definition_id = cid,
        covariate_id = covariate_ids,
        covariate_mean = round(runif(length(covariate_ids), 0.01, 0.95), 4),
        stringsAsFactors = FALSE
      )
    }))
  }))
  DatabaseConnector::insertTable(conn, tableName = p("cse_covariate_mean"), data = means_rows,
    camelCaseToSnakeCase = FALSE)

  cov_types <- c("Demographics", "Medical history", "Presentation", "prior meds", "visit context", "average")
  set.seed(123)
  sim_rows <- do.call(rbind, lapply(db_ids, function(dbid) {
    do.call(rbind, lapply(cohort_ids, function(cid1) {
      do.call(rbind, lapply(cohort_ids[cohort_ids >= cid1], function(cid2) {
        data.frame(
          database_id = dbid,
          cohort_definition_id_1 = cid1,
          cohort_definition_id_2 = cid2,
          covariate_type = cov_types,
          cosine_similarity = round(runif(length(cov_types), 0.5, 1.0), 4),
          stringsAsFactors = FALSE
        )
      }))
    }))
  }))
  DatabaseConnector::insertTable(conn, tableName = p("cse_cosine_similarity_score"), data = sim_rows,
    camelCaseToSnakeCase = FALSE)

  DatabaseConnector::insertTable(conn, tableName = p("cse_atc_level"), data = data.frame(
    drug_concept_id_1 = c(101000L, 102000L),
    drug_concept_id_2 = c(102000L, 101000L),
    level_closest_atc_relation = c(5L, 5L),
    level_furthest_atc_relation = c(2L, 2L),
    atc_1_related = c(1L, 1L),
    atc_2_related = c(1L, 1L),
    atc_3_related = c(1L, 1L),
    atc_4_related = c(1L, 1L),
    atc_5_related = c(1L, 1L),
    stringsAsFactors = FALSE
  ), camelCaseToSnakeCase = FALSE)

  invisible(dbPath)
}
