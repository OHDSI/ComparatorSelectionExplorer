# plumber API endpoint definitions for ComparatorSelectionExplorer
# Mounted by startComparatorApi()
# Shared state: qns (QueryNamespace), tablePrefix (character), isLocalhost (logical)

`%||%` <- function(x, y) if (is.null(x)) y else x
# Helper: serialises as JSON {} instead of []
emptyObject <- setNames(list(), character(0))

# Shared error handler: exposes SQL detail only on localhost deployments.
# On remote hosts the full message is logged server-side and a generic message
# is returned to the caller to avoid leaking schema/SQL information.
apiError <- function(res, e, status = 500) {
  msg <- conditionMessage(e)
  ParallelLogger::logError(msg)
  res$status <- status
  if (isTRUE(isLocalhost)) {
    list(error = msg)
  } else {
    list(error = "Internal server error")
  }
}

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
  tryCatch({
    result <- getDatabaseSources(qns)
    if (nrow(result) == 0) {
      res$status <- 404
      return(list(error = "No databases found"))
    }
    jsonlite::toJSON(result, na = "null")
  }, error = function(e) apiError(res, e))
}

#* Search cohorts by name, optionally filtered by tag
#* @param q     Search string to match against cohort name (case-insensitive). Default "" returns all.
#* @param tag   Optional. Filter cohorts by tag (e.g. "RxNorm", "ATC").
#* @get /api/cohorts
function(req, res, q = "", tag = "") {
  tryCatch({
    # Push name search and tag filter into SQL so the trigram index is used
    cohorts <- getCohortDefinitions(
      qns,
      search = if (nchar(q) > 0) q else NULL,
      tag    = if (nchar(tag) > 0) tag else NULL
    )
    if (nrow(cohorts) == 0) {
      res$status <- 404
      return(list(error = "No cohorts match the search criteria"))
    }
    jsonlite::toJSON(cohorts, na = "null")
  }, error = function(e) apiError(res, e))
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
#* @get /api/cohorts/<id:dbl>/rankings
function(req, res, id, database_ids = "", min_databases = 2,
         comparator_type = "", weight_demo = 20, weight_pres = 20,
         weight_hist = 20, weight_meds = 20, weight_visit = 20) {
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
  }, error = function(e) apiError(res, e))
}

#* Compare two cohorts in a specific database (per-domain similarity)
#* @param id1         First cohort definition ID (target)
#* @param id2         Second cohort definition ID (comparator)
#* @param database_id Database ID to compare within. If omitted, the first database that has data for this pair is used.
#* @serializer unboxedJSON
#* @get /api/cohorts/<id1:dbl>/compare/<id2:dbl>
function(req, res, id1, id2, database_id = "") {
  tryCatch({
    db_id <- if (nchar(database_id) > 0) as.numeric(database_id) else NULL

    # getDbCosineSimilarityTable requires a concrete database_id.
    # When none is supplied, resolve it from the similarity score table.
    if (is.null(db_id)) {
      available <- getDatabaseSources(qns)
      if (is.null(available) || nrow(available) == 0) {
        res$status <- 404
        return(list(error = "No databases found"))
      }
      db_id <- available$databaseId[1]
    }

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
      databaseId = db_id,
      domainScores = detail
    ), na = "null")
  }, error = function(e) apiError(res, e))
}
#* MCP Streamable HTTP endpoint
#* @post /mcp
#* @serializer unboxedJSON
function(req, res) {

  emptyObject <- setNames(list(), character(0))

  body <- tryCatch(jsonlite::fromJSON(req$postBody, simplifyVector = FALSE),
                   error = function(e) NULL)

  if (is.null(body)) {
    res$status <- 400
    return(list(jsonrpc = "2.0", id = NULL,
                error = list(code = -32700, message = "Parse error")))
  }

  id     <- body$id
  method <- body$method
  params <- body$params

  # ── initialize ────────────────────────────────────────────────────────────
  if (method == "initialize") {
    return(list(
      jsonrpc = "2.0", id = id,
      result = list(
        protocolVersion = "2024-11-05",
        capabilities    = list(tools = list(listChanged = FALSE)),
        serverInfo      = list(name = "ComparatorSelectionExplorer", version = "1.0.0")
      )
    ))
  }

  # ── tools/list ────────────────────────────────────────────────────────────
  if (method == "tools/list") {
    return(list(
      jsonrpc = "2.0", id = id,
      result = list(tools = list(

        list(name = "check_api_health",
             description = "Health check for the Comparator Selection API.",
             inputSchema = list(type = "object", properties = emptyObject)),

        list(name = "list_databases",
             description = "List available data sources.",
             inputSchema = list(type = "object", properties = emptyObject)),

        list(name = "search_cohorts",
             description = "Search exposure cohorts by name, optionally filtered by tag.",
             inputSchema = list(
               type = "object",
               properties = list(
                 q   = list(type = "string",
                            description = "Search string (case-insensitive). Default returns all."),
                 tag = list(type = "string",
                            description = "Filter by tag: 'RxNorm' for ingredients, 'ATC' for classes.")
               )
             )),

        list(name = "get_comparator_rankings",
             description = "Get ranked comparators for a target cohort based on similarity.",
             inputSchema = list(
               type     = "object",
               required = list("id"),
               properties = list(
                 id              = list(type = "number",  description = "Target cohort definition ID."),
                 database_ids    = list(type = "string",  description = "Comma-separated database IDs. Default: all."),
                 min_databases   = list(type = "number",  description = "Min databases a comparator must appear in. Default: 2."),
                 comparator_type = list(type = "string",  description = "'ATC', 'RxNorm', or '' for both."),
                 weight_demo     = list(type = "number",  description = "Demographics weight (0-100). Default: 20."),
                 weight_pres     = list(type = "number",  description = "Presentation weight (0-100). Default: 20."),
                 weight_hist     = list(type = "number",  description = "Medical history weight (0-100). Default: 20."),
                 weight_meds     = list(type = "number",  description = "Prior meds weight (0-100). Default: 20."),
                 weight_visit    = list(type = "number",  description = "Visit context weight (0-100). Default: 20.")
               )
             )),

        list(name = "compare_cohorts",
             description = "Compare two cohorts per-domain in a specific database.",
             inputSchema = list(
               type     = "object",
               required = list("id1", "id2"),
               properties = list(
                 id1         = list(type = "number", description = "Target cohort definition ID."),
                 id2         = list(type = "number", description = "Comparator cohort definition ID."),
                 database_id = list(type = "string", description = "Database ID. Omit to use first available.")
               )
             ))
      ))
    ))
  }

  # ── tools/call ────────────────────────────────────────────────────────────
  if (method == "tools/call") {
    tool_name <- params$name
    args      <- if (is.null(params$arguments)) list() else params$arguments

    tool_result <- tryCatch({

      switch(tool_name,

             "check_api_health" = {
               list(status = "ok", timestamp = Sys.time())
             },

             "list_databases" = {
               result <- getDatabaseSources(qns)
               if (nrow(result) == 0) stop("No databases found")
               result
             },

             "search_cohorts" = {
               q   <- args$q   %||% ""
               tag <- args$tag %||% ""
               cohorts <- getCohortDefinitions(
                 qns,
                 search = if (nchar(q) > 0) q else NULL,
                 tag    = if (nchar(tag) > 0) tag else NULL
               )
               if (nrow(cohorts) == 0) stop("No cohorts match the search criteria")
               cohorts
             },

             "get_comparator_rankings" = {
               id              <- as.numeric(args$id)
               database_ids    <- args$database_ids    %||% ""
               min_databases   <- as.numeric(args$min_databases   %||% 2)
               comparator_type <- args$comparator_type %||% ""
               weight_demo     <- as.numeric(args$weight_demo  %||% 20)
               weight_pres     <- as.numeric(args$weight_pres  %||% 20)
               weight_hist     <- as.numeric(args$weight_hist  %||% 20)
               weight_meds     <- as.numeric(args$weight_meds  %||% 20)
               weight_visit    <- as.numeric(args$weight_visit %||% 20)

               weights <- c(
                 Demographics      = weight_demo  / 100,
                 Presentation      = weight_pres  / 100,
                 `Medical history` = weight_hist  / 100,
                 `prior meds`      = weight_meds  / 100,
                 `visit context`   = weight_visit / 100
               )
               weightTotal <- sum(weights)
               if (weightTotal > 0) weights <- round(weights / weightTotal, 2) else weights <- NULL

               raw <- getCohortSimilarityScores(qns, targetCohortId = id, weights = weights)
               if (nrow(raw) == 0) stop("No similarity scores found for this target cohort")

               if (nchar(comparator_type) > 0) {
                 raw <- raw[raw$isAtc2 == (tolower(comparator_type) == "atc"), , drop = FALSE]
                 if (nrow(raw) == 0) stop(paste("No comparators found of type:", comparator_type))
               }

               db_ids <- if (nchar(database_ids) > 0) trimws(strsplit(database_ids, ",")[[1]]) else unique(raw$databaseId)
               raw <- raw[raw$databaseId %in% db_ids, , drop = FALSE]

               filtered <- raw |>
                 dplyr::group_by(.data$cohortDefinitionId2) |>
                 dplyr::filter(dplyr::n() >= min_databases) |>
                 dplyr::ungroup()
               if (nrow(filtered) == 0) stop(paste("No comparators found in at least", min_databases, "databases"))

               filtered |>
                 dplyr::group_by(.data$cohortDefinitionId2, .data$shortName, .data$isAtc2) |>
                 dplyr::summarise(nDatabases = dplyr::n(), avgSimilarity = mean(.data$cosineSimilarity), .groups = "drop") |>
                 dplyr::arrange(dplyr::desc(.data$avgSimilarity)) |>
                 dplyr::mutate(rank = dplyr::row_number(),
                               comparatorType = ifelse(.data$isAtc2, "ATC", "RxNorm")) |>
                 dplyr::rename(comparatorId = "cohortDefinitionId2", comparatorName = "shortName")
             },

             "compare_cohorts" = {
               id1         <- as.numeric(args$id1)
               id2         <- as.numeric(args$id2)
               database_id <- args$database_id %||% ""
               db_id <- if (nchar(database_id) > 0) as.numeric(database_id) else {
                 available <- getDatabaseSources(qns)
                 if (is.null(available) || nrow(available) == 0) stop("No databases found")
                 available$databaseId[1]
               }
               detail <- getDbCosineSimilarityTable(qns, targetCohortId = id1,
                                                    comparatorCohortId = id2,
                                                    databaseId = db_id,
                                                    weights = NULL, returnReactable = FALSE)
               if (is.null(detail) || nrow(detail) == 0) stop("No similarity data found for this cohort pair")
               detail
             },

             stop(paste("Unknown tool:", tool_name))
      )

    }, error = function(e) {
      ParallelLogger::logError(conditionMessage(e))
      structure(list(message = conditionMessage(e)), class = "mcp_tool_error")
    })

    if (inherits(tool_result, "mcp_tool_error")) {
      return(list(
        jsonrpc = "2.0", id = id,
        result = list(
          content = list(list(type = "text", text = tool_result$message)),
          isError = TRUE
        )
      ))
    }

    return(list(
      jsonrpc = "2.0", id = id,
      result = list(
        content = list(list(
          type = "text",
          text = jsonlite::toJSON(tool_result, auto_unbox = TRUE, na = "null")
        ))
      )
    ))
  }

  # ── notifications (fire-and-forget) ───────────────────────────────────────
  if (startsWith(method, "notifications/")) {
    res$status <- 204
    return(invisible(NULL))
  }

  # ── unknown method ────────────────────────────────────────────────────────
  res$status <- 400
  list(jsonrpc = "2.0", id = id,
       error = list(code = -32601, message = paste("Method not found:", method)))
}
