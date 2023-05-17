covariateUi <- function(id = "") {
  if (id == "") {
    ns <- function(x) { x }
  } else {
    ns <- shiny::NS(id)
  }
  shiny::tagList(
    h3(strong("Visualizations")),
    shiny::fluidRow(
      shiny::column(
        width = 6,
        h6(em("Covariate prevalence")),
        shinycssloaders::withSpinner(
          plotly::plotlyOutput(
            outputId = ns("scatterPlot")
          )
        )
      ),
      shiny::column(
        width = 6,
        h6(em("Standardized mean differences")),
        shinycssloaders::withSpinner(
          plotly::plotlyOutput(
            outputId = ns("smdPlot")
          )
        )
      )
    ),
    # display table
    h3(strong("Covariate Tables")),
    tabsetPanel(
      tabPanel(
        title = "Demographics",
        h4(strong("Demographics")),
        textOutput(ns("covTableDemoBalance")),
        shinycssloaders::withSpinner(reactable::reactableOutput(ns("covTableDemo")))
      ),
      tabPanel(
        title = "Presentation",
        h4(strong("Presentation")),
        h5(em("One covariate per condition observed in 30 days prior to index")),
        textOutput(ns("covTablePresBalance")),
        shinycssloaders::withSpinner(reactable::reactableOutput(ns("covTablePres")))
      ),
      tabPanel(
        title = "Medical history",
        h4(strong("Medical history")),
        h5(em("One covariate per condition observed more than 30 days prior to index")),
        textOutput(ns("covTableMhistBalance")),
        shinycssloaders::withSpinner(reactable::reactableOutput(ns("covTableMhist")))
      ),
      tabPanel(
        title = "Prior medications",
        h4(strong("Prior medications")),
        h5(em("One covariate per RxNorm ingredient observed more than 30 days prior to index")),
        textOutput(ns("covTablePmedsBalance")),
        shinycssloaders::withSpinner(reactable::reactableOutput(ns("covTablePmeds")))
      ),
      tabPanel(
        title = "Visit context",
        h4(strong("Visit context")),
        h5(em("Inpatient and emergency department visits observed in 30 days prior to index")),
        textOutput(ns("covTableVisitBalance")),
        shinycssloaders::withSpinner(reactable::reactableOutput(ns("covTableVisit")))
      ),
      tabPanel(
        title = "Index date",
        h4(strong("Index date")),
        h5(em("Variables observed on the same date as exposure index")),
        p("Note that these covariates are not used in cacluation of similarity scores. Many will likely bias propensity
        score matching and should be excluded from models."),
        shinycssloaders::withSpinner(reactable::reactableOutput(ns("covTableIndex")))
      )
    )
  )
}

withTooltip <- function(value, tooltip, ...) {
  shiny::div(style = "text-decoration: underline; text-decoration-style: dotted; cursor: help",
             tippy::tippy(value, tooltip, ...))
}


createCovariateReactable <- function(tableData, targetName, comparatorName, fmtSmd) {
  checkmate::assertDataFrame(tableData)
  checkmate::assertNames(colnames(tableData), must.include = c("covariateShortName", "mean1", "mean2", "stdDiff"))

  reactable::reactable(
    data = tableData,
    columns = list(
      "covariateShortName" = reactable::colDef(name = "Covariate", align = "right", vAlign = "bottom"),
      "mean1" = reactable::colDef(name = targetName, cell = function(value) { ifelse(value >= 0.01, percent(value, accuracy = 0.1), "<1%") }, align = "center", vAlign = "bottom"),
      "mean2" = reactable::colDef(name = comparatorName, cell = function(value) { ifelse(value >= 0.01, percent(value, accuracy = 0.1), "<1%") }, align = "center", vAlign = "bottom"),
      "stdDiff" = reactable::colDef(
        name = "Std. Diff.",

        cell = function(value, index) {

          if (tableData$mean1[index] >= 0.01 & tableData$mean2[index] >= 0.01) {

            sprintf(fmtSmd, value)

          } else {

            ifelse(tableData$mean1[index] < 0.01, paste0("(\u2265) ", sprintf(fmtSmd, value)), paste0("(\u2264) ", sprintf(fmtSmd, value))) } },
        align = "center",
        vAlign = "bottom")),
    bordered = TRUE,
    searchable = TRUE,
    showPageSizeOptions = TRUE,
    pageSizeOptions = c(5, 10, 20, 50, 100, 1000),
    striped = TRUE,
    highlight = TRUE,
    compact = TRUE,
    theme = reactable::reactableTheme(
      borderColor = "#dfe2e5",
      stripedColor = "#f6f8fa",
      highlightColor = "#eab676",
      cellPadding = "8px 12px",
      searchInputStyle = list(width = "100%")),
    showSortIcon = TRUE)
}

renderCovariateReactable <- function(covariateType,
                                     cohortDefinitionReactive,
                                     covariateDataReactive,
                                     selectedExposure,
                                     selectedComparator,
                                     fmtSmd,
                                     covariateReplaceString = "",
                                     stringToSentence = FALSE) {
  cohortDefinitions <- cohortDefinitionReactive()
  # get data
  covData <- covariateDataReactive()

  # create column names with cohort sample sizes
  targetName <- paste0(
    cohortDefinitions$shortName[cohortDefinitions$cohortDefinitionId == selectedExposure()],
    " (n = ",
    prettyNum(first(covData$n1), big.mark = ","),
    ")")

  comparatorName <- paste0(
    cohortDefinitions$shortName[cohortDefinitions$cohortDefinitionId == selectedComparator()],
    " (n = ",
    prettyNum(first(covData$n2), big.mark = ","),
    ")")

  # subset data and select relevant columns
  tableData <- covData %>%
    dplyr::filter(.data$covariateType == !!covariateType) %>%
    dplyr::arrange(desc(abs(.data$stdDiff))) %>%
    dplyr::select("covariateShortName", "mean1", "mean2", "stdDiff")

  if (covariateReplaceString != "") {
    tableData <- tableData %>% dplyr::mutate(covariateShortName = gsub(covariateReplaceString, "", .data$covariateShortName))
  }

  if (stringToSentence) {
    tableData <- tableData %>% dplyr::mutate(covariateShortName = stringr::str_to_sentence(.data$covariateShortName))
  }

  createCovariateReactable(tableData, targetName, comparatorName, fmtSmd)

}