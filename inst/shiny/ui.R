library(shiny)

shinyUI(fluidPage(

  # Application title
  titlePanel(
    title = "Comparator Selection Explorer",
    windowTitle = "Comparator Selection Explorer"),
  p("Janssen Research & Development"),
  tabsetPanel( # tabs panel
    type = "pills",
    tabPanel( # tab 1: multi-source explorer
      title = "Recommend Comparators",
      shinydashboard::box(
        title = "Settings",
        width = 12,
        fluidRow(
          column(
            width = 6,
            selectizeInput(
              inputId = "selectedExposure",
              choices = NULL,
              width = "100%",
              label = "Select target exposure:"),
            selectInput(
              inputId = "selectedComparatorTypes",
              label = "Select comparator types:",
              width = "100%",
              choices = c("RxNorm Ingredients", "ATC Classes"),
              selected = "RxNorm Ingredients",
              multiple = TRUE
            )
          ),
          column(
            width = 3,
            checkboxGroupInput(
              inputId = "selectedDatabases",
              label = "Select data sources:",
              choices = NULL,
              selected = NULL,
              inline = FALSE,
              width = NULL,
              choiceNames = NULL,
              choiceValues = NULL
            )
          ),
          column(
            width = 3,
            sliderInput(
              inputId = "minNumDatabases",
              label = "Minimum data sources with comparator presence:",
              min = 1,
              max = 10,
              value = 2,
              step = 1,
              ticks = FALSE),
            radioButtons(
              inputId = "avgOn",
              label = "Rank comparators on:",
              choices = c("Average similarity score", "Average source-specific rank"),
              selected = "Average similarity score"),

          ),
          fluidRow(
            column(
              width = 12,
              shiny::actionButton(inputId = "getResults", "Suggest Comparators")
            )
          ),
        ),
        shiny::conditionalPanel(
          condition = "input.getResults > 0",
          shinydashboard::box(
            width = 12,
            title = "Comparator listing",
            conditionalPanel(
              "input.selectedExposure != ''",
              shiny::actionButton(inputId = "showRankings", "Show rank plot"),
              br()
            ),
            shinycssloaders::withSpinner(reactable::reactableOutput("multiDatabaseSimTable"))
          )
        )
      ),
    ),
    tabPanel( # tab 3: about
      title = "About",
      fluidRow(
        column(
          width = 12,
          h3("Description"),
          htmlTemplate("about.html"),
          h3("Currently Available Data Sources"),
          shinycssloaders::withSpinner(
            reactable::reactableOutput("dataSources")
          ),
          h3("License"),
          htmlTemplate("license.html"),
        )
      )
    )
  )
))
