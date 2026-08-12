#' Create a Comparator Selection API client
#'
#' Returns a list of convenience functions for interacting with a running
#' ComparatorSelectionExplorer Plumber API.
#'
#' @param host   String. API host (default "127.0.0.1")
#' @param port   Integer. API port (default 8080)
#'
#' @return A list with methods: health(), databases(), search(), rankings(), compare().
#'
#' @export
#'
#' @examples
#' \dontrun{
#' client <- createApiClient("localhost", 8080)
#' client$health()
#' client$search("ibuprofen")
#' client$rankings(101, min_databases = 2)
#' }
createApiClient <- function(host = "127.0.0.1", port = 8080) {
  baseUrl <- paste0("http://", host, ":", port)

  getJson <- function(path, query = list()) {
    query <- Filter(function(x) !is.null(x) && nchar(x) > 0, query)
    url <- httr::modify_url(baseUrl, path = path, query = query)
    resp <- httr::GET(url, httr::accept_json())
    if (httr::http_error(resp)) {
      content <- tryCatch(httr::content(resp, as = "parsed", simplifyVector = TRUE),
                          error = function(e) list(error = httr::http_status(resp)$message))
      stop(content$error %||% httr::http_status(resp)$message, call. = FALSE)
    }
    httr::content(resp, as = "parsed", simplifyVector = TRUE)
  }

  list(

    health = function() {
      getJson("/api/health")
    },

    databases = function() {
      getJson("/api/databases")
    },

    search = function(q = "", tag = "") {
      getJson("/api/cohorts", query = list(q = q, tag = tag))
    },

    rankings = function(targetCohortId,
                        database_ids = NULL,
                        min_databases = 2,
                        comparator_type = "",
                        weight_demo = 20,
                        weight_pres = 20,
                        weight_hist = 20,
                        weight_meds = 20,
                        weight_visit = 20) {
      path <- paste0("/api/cohorts/", targetCohortId, "/rankings")
      query <- list(
        database_ids = if (length(database_ids) > 0) paste(database_ids, collapse = ",") else "",
        min_databases = min_databases,
        comparator_type = comparator_type,
        weight_demo = weight_demo,
        weight_pres = weight_pres,
        weight_hist = weight_hist,
        weight_meds = weight_meds,
        weight_visit = weight_visit
      )
      getJson(path, query = query)
    },

    compare = function(cohortId1, cohortId2, database_id = "") {
      path <- paste0("/api/cohorts/", cohortId1, "/compare/", cohortId2)
      getJson(path, query = list(database_id = database_id))
    }
  )
}
