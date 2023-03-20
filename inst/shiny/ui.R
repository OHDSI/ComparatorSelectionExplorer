library(shiny)

shinyUI(fluidPage(

  # Application title
  titlePanel(
    title = "Comparator Selection Explorer",
    windowTitle = "Comparator Selection Explorer"),
  p("Janssen Research & Development"),

  tabsetPanel( # tabs panel
    type = "pills",
    tabPanel( # tab 1: single-source explorer
      title = "Explore Single Source",
      sidebarLayout(
        sidebarPanel(
          h4(strong("Settings")),
          selectizeInput(
            inputId = "selectedDatabase",
            choices = NULL,
            label = "Select target data source:"),
          selectizeInput(
            inputId = "selectedExposure",
            choices = NULL,
            label = "Select target exposure:"),
          selectInput(inputId = "selectedComparatorTypes",
                      label = "Select comparator type(s):",
                      choices = c("RxNorm Ingredients", "ATC Classes"),
                      selected = "RxNorm Ingredients",
                      multiple = TRUE),
          h4(strong("Visualizations")),
          h6(em("Similarity scores by domain-specific ranking")),
          shinycssloaders::withSpinner(
            plotOutput(
              outputId = "stepPlot"),
          ),
          conditionalPanel(
            condition = "output.selectedComparator == true",
            h6(em("Covariate prevalence")),
            shinycssloaders::withSpinner(
              plotly::plotlyOutput(
                outputId = "scatterPlot"
              )
            ),
            h6(em("Standardized mean differences")),
            shinycssloaders::withSpinner(
              plotly::plotlyOutput(
                outputId = "smdPlot"
              )
            )
          )
        ),

        # display table
        mainPanel(
          h3("Comparator listing"),
          textOutput("selectedCohortInfo"),
          p("Select comparator to view covariate distributions"),
          shinycssloaders::withSpinner(reactable::reactableOutput("cosineSimilarityTbl")),

          conditionalPanel(
            condition = "output.selectedComparator == true",
            h3(strong("Distribution of covariates")),
            h4(strong("Demographics")),
            textOutput("covTableDemoBalance"),
            shinycssloaders::withSpinner(reactable::reactableOutput("covTableDemo")),
            h4(strong("Presentation")),
            h5(em("One covariate per condition observed in 30 days prior to index")),
            textOutput("covTablePresBalance"),
            shinycssloaders::withSpinner(reactable::reactableOutput("covTablePres")),
            h4(strong("Medical history")),
            h5(em("One covariate per condition observed more than 30 days prior to index")),
            textOutput("covTableMhistBalance"),
            shinycssloaders::withSpinner(reactable::reactableOutput("covTableMhist")),
            h4(strong("Prior medications")),
            h5(em("One covariate per RxNorm ingredient observed more than 30 days prior to index")),
            textOutput("covTablePmedsBalance"),
            shinycssloaders::withSpinner(reactable::reactableOutput("covTablePmeds")),
            h4(strong("Visit context")),
            h5(em("Inpatient and emergency department visits observed in 30 days prior to index")),
            textOutput("covTableVisitBalance"),
            shinycssloaders::withSpinner(reactable::reactableOutput("covTableVisit"))
          ),
        )
      )
    ),
    tabPanel( # tab 2: multi-source explorer
      title = "Synthesize Across Sources",
      sidebarLayout(
        sidebarPanel(
          h4(strong("Settings")),
          selectizeInput(
            inputId = "selectedExposure2",
            choices = NULL,
            label = "Select target exposure:"),
          selectInput(
            inputId = "selectedComparatorTypes",
            label = "Select comparator type(s):",
            choices = c("RxNorm Ingredients", "ATC Classes"),
            selected = "RxNorm Ingredients",
            multiple = TRUE),
          checkboxGroupInput(
            inputId = "selectedDatabases",
            label = "Select data source(s):",
            choices = NULL,
            selected = NULL,
            inline = FALSE,
            width = NULL,
            choiceNames = NULL,
            choiceValues = NULL),
          sliderInput(
            inputId = "minNumDatabases",
            label = "Require comparator presence in at least x databases:",
            min = 1,
            max = 10, # placeholder
            value = 1,
            step = 1),
          radioButtons(
            inputId = "avgOn",
            label = "Rank comparators on:",
            choices = c("Average similarity score", "Average source-specific rank"),
            selected = "Average similarity score")),
        mainPanel(
          h3("Comparator listing"),
          shinycssloaders::withSpinner(reactable::reactableOutput("multiDatabaseSimTable"))
        )),
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
