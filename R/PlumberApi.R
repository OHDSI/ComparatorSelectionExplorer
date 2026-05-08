#' Start the Comparator Selection Plumber API
#'
#' Launches a REST API that provides cohort search and comparator ranking endpoints.
#' Reuses the query functions from the Shiny module's results queries.
#'
#' @param connectionDetails DatabaseConnector connection details object
#' @param resultsSchema     String. Results database schema
#' @param tablePrefix       String. Table prefix for results tables (default "")
#' @param port              Integer. Port to run the API on (default 8080)
#' @param host              String. Host to bind to (default "127.0.0.1")
#'
#' @return Invisibly returns the plumber router object. Called for its side effect
#'         of starting the API server.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' startComparatorApi(connectionDetails, "results_schema", tablePrefix = "cse_")
#' }
startComparatorApi <- function(connectionDetails,
                                resultsSchema,
                                tablePrefix = "",
                                port = 8080,
                                host = "127.0.0.1") {
  if (!requireNamespace("plumber", quietly = TRUE)) {
    stop("The 'plumber' package is required to run the API. Install with: install.packages(\"plumber\")", call. = FALSE)
  }

  qns <- createResultsQueryNamespace(
    connectionDetails = connectionDetails,
    resultsSchema = resultsSchema,
    tablePrefix = tablePrefix
  )

  apiPath <- system.file("plumber", "api.R", package = "ComparatorSelectionExplorer")
  pr <- plumber::plumb(apiPath)
  pr$set_shared("qns", qns)
  pr$set_shared("tablePrefix", tablePrefix)

  pr$registerHook("exit", function() {
    qns$closeConnection()
  })

  plumber::pr_run(pr, host = host, port = port)
}
