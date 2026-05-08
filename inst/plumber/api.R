# plumber API endpoint definitions for ComparatorSelectionExplorer
# Mounted by startComparatorApi()
# Shared state: qns (QueryNamespace), tablePrefix (character)

#* @apiTitle Comparator Selection Explorer API
#* @apiDescription REST API for searching cohorts and retrieving comparator rankings

#* Health check
#* @get /api/health
function(res) {
  list(status = "ok", timestamp = Sys.time())
}

#* List available data sources
#* @get /api/databases
function(req, res) {
  qns <- req$pr$get_shared("qns")
  tryCatch({
    result <- getDatabaseSources(qns)
    if (nrow(result) == 0) {
      res$status <- 404
      return(list(error = "No databases found"))
    }
    jsonlite::toJSON(result, na = "null")
  }, error = function(e) {
    res$status <- 500
    list(error = conditionMessage(e))
  })
}

#* Search cohorts by name, optionally filtered by tag
#* @param q     Search string to match against cohort name (case-insensitive). Default "" returns all.
#* @param tag   Optional. Filter cohorts by tag (e.g. "RxNorm", "ATC").
#* @get /api/cohorts
function(req, res, q = "", tag = "") {
  qns <- req$pr$get_shared("qns")
  tryCatch({
    cohorts <- getCohortDefinitions(qns)
    if (nrow(cohorts) == 0) {
      res$status <- 404
      return(list(error = "No cohorts found"))
    }
    if (nchar(q) > 0) {
      cohorts <- cohorts[grepl(q, cohorts$shortName, ignore.case = TRUE), , drop = FALSE]
    }
    if (nchar(tag) > 0) {
      tagDf <- getCohortTags(qns, cohortIds = cohorts$cohortDefinitionId)
      cohortIdsWithTag <- tagDf$cohortDefinitionId[tagDf$tag == tag]
      cohorts <- cohorts[cohorts$cohortDefinitionId %in% cohortIdsWithTag, , drop = FALSE]
    }
    if (nrow(cohorts) == 0) {
      res$status <- 404
      return(list(error = "No cohorts match the search criteria"))
    }
    jsonlite::toJSON(cohorts, na = "null")
  }, error = function(e) {
    res$status <- 500
    list(error = conditionMessage(e))
  })
}

#* Get comparator rankings for a target cohort
#* @param id              Target cohort definition ID
#* @param database_ids    Optional. Comma-separated list of database IDs to include. Default: all.
#* @param min_databases   Minimum number of databases a comparator must appear in (default: 2)
#* @param comparator_type Optional. Filter by type: "ATC", "RxNorm", or "" for both (default)
#* @param weight_demo     Weight for Demographics domain (0-100, default: 20)
#* @param weight_pres     Weight for Presentation domain (0-100, default: 20)
#* @param weight_hist     Weight for Medical history domain (0-100, default: 20)
#* @param weight_meds     Weight for Prior meds domain (0-100, default: 20)
#* @param weight_visit    Weight for Visit context domain (0-100, default: 20)
#* @serializer unboxedJSON
#* @get /api/cohorts/<id:int>/rankings
function(req, res, id, database_ids = "", min_databases = 2,
         comparator_type = "", weight_demo = 20, weight_pres = 20,
         weight_hist = 20, weight_meds = 20, weight_visit = 20) {
  qns <- req$pr$get_shared("qns")
  tryCatch({
    weights <- c(
      Demographics = as.numeric(weight_demo) / 100,
      Presentation = as.numeric(weight_pres) / 100,
      `Medical history` = as.numeric(weight_hist) / 100,
      `prior meds` = as.numeric(weight_meds) / 100,
      `visit context` = as.numeric(weight_visit) / 100
    )
    weightTotal <- sum(weights)
    if (weightTotal > 0) {
      weights <- round(weights / weightTotal, 2)
    } else {
      weights <- NULL
    }

    raw <- getCohortSimilarityScores(qns, targetCohortId = id, weights = weights)

    if (nrow(raw) == 0) {
      res$status <- 404
      return(list(error = "No similarity scores found for this target cohort"))
    }

    if (nchar(comparator_type) > 0) {
      atc_val <- tolower(comparator_type) == "atc"
      raw <- raw[raw$isAtc2 == atc_val, , drop = FALSE]
      if (nrow(raw) == 0) {
        res$status <- 404
        return(list(error = paste("No comparators found of type:", comparator_type)))
      }
    }

    db_ids <- if (nchar(database_ids) > 0) {
      trimws(strsplit(database_ids, ",")[[1]])
    } else {
      unique(raw$databaseId)
    }
    raw <- raw[raw$databaseId %in% db_ids, , drop = FALSE]

    min_db <- as.numeric(min_databases)

    filtered <- raw |>
      dplyr::arrange(.data$databaseId, .data$cdmSourceAbbreviation, dplyr::desc(.data$cosineSimilarity)) |>
      dplyr::group_by(.data$databaseId, .data$cdmSourceAbbreviation) |>
      dplyr::mutate(
        cdmSpecificRank = dplyr::row_number(),
        comparatorsInCdm = dplyr::n_distinct(.data$cohortDefinitionId2)
      ) |>
      dplyr::ungroup() |>
      dplyr::group_by(.data$cohortDefinitionId2) |>
      dplyr::filter(dplyr::n() >= min_db) |>
      dplyr::ungroup()

    if (nrow(filtered) == 0) {
      res$status <- 404
      return(list(error = paste("No comparators found in at least", min_db, "databases")))
    }

    ranked <- filtered |>
      dplyr::group_by(.data$cohortDefinitionId2, .data$shortName, .data$isAtc2) |>
      dplyr::summarise(
        nDatabases = dplyr::n(),
        avgSimilarity = mean(.data$cosineSimilarity),
        avgCdmRank = mean(.data$cdmSpecificRank),
        .groups = "drop"
      ) |>
      dplyr::arrange(dplyr::desc(.data$avgSimilarity)) |>
      dplyr::mutate(rank = dplyr::row_number())

    result <- ranked |>
      dplyr::mutate(
        isAtc2 = as.logical(.data$isAtc2),
        comparatorType = ifelse(.data$isAtc2, "ATC", "RxNorm")
      ) |>
      dplyr::select(
        "rank", "cohortDefinitionId2", "shortName", "comparatorType",
        "avgSimilarity", "avgCdmRank", "nDatabases"
      ) |>
      dplyr::rename(
        comparatorId = "cohortDefinitionId2",
        comparatorName = "shortName"
      )

    jsonlite::toJSON(list(
      targetCohortId = id,
      totalComparators = nrow(result),
      rankings = result
    ), na = "null")
  }, error = function(e) {
    res$status <- 500
    list(error = conditionMessage(e))
  })
}

#* Compare two cohorts in a specific database (per-domain similarity)
#* @param id1         First cohort definition ID (target)
#* @param id2         Second cohort definition ID (comparator)
#* @param database_id Database ID to compare within
#* @serializer unboxedJSON
#* @get /api/cohorts/<id1:int>/compare/<id2:int>
function(req, res, id1, id2, database_id = "") {
  qns <- req$pr$get_shared("qns")
  tryCatch({
    db_id <- if (nchar(database_id) > 0) database_id else NULL

    detail <- getDbCosineSimilarityTable(
      qns,
      targetCohortId = id1,
      comparatorCohortId = id2,
      databaseId = db_id,
      weights = NULL,
      returnReactable = FALSE
    )

    if (is.null(detail) || nrow(detail) == 0) {
      res$status <- 404
      return(list(error = "No similarity data found for this cohort pair"))
    }

    jsonlite::toJSON(list(
      targetCohortId = id1,
      comparatorCohortId = id2,
      databaseId = if (is.null(db_id)) "all" else db_id,
      domainScores = detail
    ), na = "null")
  }, error = function(e) {
    res$status <- 500
    list(error = conditionMessage(e))
  })
}
