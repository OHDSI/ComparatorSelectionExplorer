covariateUi <- function(id = "") {
  if (id == "") {
    ns <- function(x) {x}
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
      )
    )
  )
}

withTooltip <- function(value, tooltip, ...) {
  shiny::div(style = "text-decoration: underline; text-decoration-style: dotted; cursor: help",
             tippy::tippy(value, tooltip, ...))
}