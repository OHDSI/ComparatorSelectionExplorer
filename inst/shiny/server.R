# packages
library(shiny)
library(ggplot2)
library(scales)
library(tidyr)
library(dplyr, warn.conflicts = FALSE)

# decimal formatters
fmtSim <- "%.3f"
fmtSmd <- "%.2f"

dataModelSpec <- ResultModelManager::loadResultsDataModelSpecifications("resultsDataModel.csv")
qns <- ResultModelManager::createQueryNamespace(connectionDetails = connectionDetails,
                                                usePooledConnection = TRUE,
                                                schema = resultsSchema,
                                                tablePrefix = tablePrefix,
                                                tableSpecification = dataModelSpec)


withTooltip <- function(value, tooltip, ...) {
  shiny::div(style = "text-decoration: underline; text-decoration-style: dotted; cursor: help",
             tippy::tippy(value, tooltip, ...))
}


modalUi <- function() {
  shiny::tagList(
    h3(strong("Visualizations")),
    tabsetPanel(
      tabPanel(
        title = "Covariate prevalence",
        h6(em("Covariate prevalence")),
        shinycssloaders::withSpinner(
          plotly::plotlyOutput(
            outputId = "scatterPlot"
          )
        )
      ),
      tabPanel(
        title = "Standardized mean differences",
        h6(em("Standardized mean differences")),
        shinycssloaders::withSpinner(
          plotly::plotlyOutput(
            outputId = "smdPlot"
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
        textOutput("covTableDemoBalance"),
        shinycssloaders::withSpinner(reactable::reactableOutput("covTableDemo"))
      ),
      tabPanel(
        title = "Presentation",
        h4(strong("Presentation")),
        h5(em("One covariate per condition observed in 30 days prior to index")),
        textOutput("covTablePresBalance"),
        shinycssloaders::withSpinner(reactable::reactableOutput("covTablePres"))
      ),
      tabPanel(
        title = "Medical history",
        h4(strong("Medical history")),
        h5(em("One covariate per condition observed more than 30 days prior to index")),
        textOutput("covTableMhistBalance"),
        shinycssloaders::withSpinner(reactable::reactableOutput("covTableMhist"))
      ),
      tabPanel(
        title = "Prior medications",
        h4(strong("Prior medications")),
        h5(em("One covariate per RxNorm ingredient observed more than 30 days prior to index")),
        textOutput("covTablePmedsBalance"),
        shinycssloaders::withSpinner(reactable::reactableOutput("covTablePmeds"))
      ),
      tabPanel(
        title = "Visit context",
        h4(strong("Visit context")),
        h5(em("Inpatient and emergency department visits observed in 30 days prior to index")),
        textOutput("covTableVisitBalance"),
        shinycssloaders::withSpinner(reactable::reactableOutput("covTableVisit"))
      )
    )
  )
}

# Define server logic required to draw a histogram
shinyServer(function(input, output, session) {

  selectedDatabase <- shiny::reactive({
    input$show_details$index[2]
  })

  getCohortDefinitions <- shiny::reactive({
    qns$queryDb("select distinct
               t.cohort_definition_id,
               short_name,
               atc_flag as is_atc
             from @schema.@cohort_definition t
             where t.cohort_definition_id is not null
             and   atc_flag in (0, 1)
             order by short_name")
  })

  selectedComparator <- shiny::reactive({
    input$show_details$index[1]
  })

  # initial query to get list of cohort definitions
  getCohortDefinitionsWithCounts <- shiny::reactive({
    dbSel <- selectedDatabase()
    if (is.null(dbSel) || dbSel == "")
      return(data.frame())

    qns$queryDb(
      sql = "select distinct
               t.cohort_definition_id,
               short_name,
               atc_flag as is_atc,
               c.num_persons,
               c.database_id
             from @schema.@cohort_definition t
             inner join @schema.@cohort_count c ON c.cohort_definition_id = t.cohort_definition_id
             where t.cohort_definition_id is not null
             and   atc_flag in (0, 1)
             and c.database_id = @database_id
             order by short_name",
      database_id = dbSel
    )
  })

  getDatabaseSources <- shiny::reactive({
    qns$queryDb(sql = "select distinct * from @schema.@cdm_source_info t")
  })

  observe({
    shiny::withProgress({
      dbSources <- getDatabaseSources()
      dbChoices <- dbSources$databaseId
      names(dbChoices) <- dbSources$cdmSourceAbbreviation
      dbChoices <- dbChoices[order(names(dbChoices))]

      updateSelectizeInput(
        session,
        "selectedDatabase",
        choices = dbChoices,
        # default choice: CCAE
        selected = dbChoices[which(names(dbChoices) == "IBM CCAE")],
        server = TRUE)
    }, message = "Loading database sources")
  })

  observe({
    shiny::withProgress({

      dbSources <- getDatabaseSources()
      dbChoices <- dbSources$databaseId
      names(dbChoices) <- dbSources$cdmSourceAbbreviation
      dbChoices <- dbChoices[order(names(dbChoices))]

      updateCheckboxGroupInput(
        session,
        "selectedDatabases",
        choices = dbChoices,
        # default choice: everything except optum DoD, since it's duplicative with SES
        selected = dbChoices[which(names(dbChoices) != "OPTUM Extended DOD")])
    }, message = "Loading database sources")
  })

  observe({
    shiny::withProgress({
      dbSources <- getDatabaseSources()
      dbChoices <- dbSources$databaseId

      updateSliderInput(
        session,
        inputId = "minNumDatabases",
        min = 1,
        max = length(dbChoices),
        step = 1)
    }, message = "Loading database sources")
  })


  observe({
    shiny::withProgress({
      cohortDefinitions <- getCohortDefinitions()
      if (nrow(cohortDefinitions)) {

        exposureSelection <- cohortDefinitions$cohortDefinitionId
        names(exposureSelection) <- cohortDefinitions$shortName
        updateSelectizeInput(
          session,
          "selectedExposure",
          choices = exposureSelection,
          selected = 8826,
          server = TRUE)
      }
    }, message = "Loading cohort definitions")
  })

  observe({
    shiny::withProgress({
      cohortDefinitions <- getCohortDefinitions()
      if (nrow(cohortDefinitions)) {

        exposureSelection <- cohortDefinitions$cohortDefinitionId
        names(exposureSelection) <- cohortDefinitions$shortName
        updateSelectizeInput(
          session,
          "selectedExposure",
          choices = exposureSelection,
          selected = 8826,
          server = TRUE)
      }
    }, message = "Loading cohort definitions")
  })


  #### ---- function to get cosine similarity data for single database-target pair ---- ####
  getDbSimilarity <- shiny::reactive({
    # identify target cohort
    targetCohortId <- input$selectedExposure
    validate(need(input$selectedExposure, "must select exposure"))

    shiny::withProgress({
      # identify selected comparator types
      if (length(input$selectedComparatorTypes) == 2L) { atcSelection <- c(0, 1) }
      else if (input$selectedComparatorTypes == "RxNorm Ingredients") { atcSelection <- c(0) }
      else if (input$selectedComparatorTypes == "ATC Classes") { atcSelection <- c(1) }
      # send query to get results data
      resultsData <- qns$queryDb(
        sql = "
            select distinct
               d.cdm_source_abbreviation,
               CASE
                  WHEN t.cohort_definition_id_1 = @targetCohortId THEN t.cohort_definition_id_2
                  ELSE t.cohort_definition_id_1
               END as cohort_definition_id_2,

               CASE
                  WHEN t.cohort_definition_id_1 = @targetCohortId THEN cd2.atc_flag
                  ELSE cd.atc_flag
               END as is_atc_2,

               CASE
                  WHEN t.cohort_definition_id_1 = @targetCohortId THEN cd2.short_name
                  ELSE cd.short_name
               END as short_name,
               cosine_similarity,
               CASE
                  WHEN t.cohort_definition_id_1 = @targetCohortId THEN ec.num_persons
                  ELSE ec2.num_persons
               END as num_persons,
               t.covariate_type
             from @schema.@cosine_similarity_score t
             inner join @schema.@cohort_count ec ON ec.cohort_definition_id = t.cohort_definition_id_2 and ec.database_id = t.database_id
             inner join @schema.@cohort_count ec2 ON ec2.cohort_definition_id = t.cohort_definition_id_1 and ec2.database_id = t.database_id
             inner join @schema.@cohort_definition cd ON cd.cohort_definition_id = t.cohort_definition_id_1
             inner join @schema.@cohort_definition cd2 ON cd2.cohort_definition_id = t.cohort_definition_id_2
             inner join @schema.@cdm_source_info d ON t.database_id = d.database_id
             where (t.cohort_definition_id_1 = @targetCohortId or t.cohort_definition_id_2 = @targetCohortId)
             and t.database_id IN (@database_ids)
           ",
        targetCohortId = targetCohortId,
        database_ids = input$selectedDatabases)
    }, message = "Loading similarity scores", value = 0.5)

    resultsData %>%
      dplyr::filter(.data$isAtc2 %in% atcSelection)

  })

  #### ---- function to get cosine similarity data for target in all databases ---- ####
  getSimilarityAllDatabases <- shiny::reactive({
    # identify target cohort
    targetCohortId <- input$selectedExposure
    validate(need(input$selectedExposure, "must select exposure"))

    shiny::withProgress({
      # identify selected comparator types
      if (length(input$selectedComparatorTypes) == 2L) { atcSelection <- c(0, 1) }
      else if (input$selectedComparatorTypes == "RxNorm Ingredients") { atcSelection <- c(0) }
      else if (input$selectedComparatorTypes == "ATC Classes") { atcSelection <- c(1) }

      # send query to get results data
      resultsData <- qns$queryDb(
        sql = "
            select distinct
               csi.database_id,
               csi.cdm_source_abbreviation,
               CASE
                  WHEN t.cohort_definition_id_1 = @targetCohortId THEN t.cohort_definition_id_2
                  ELSE t.cohort_definition_id_1
               END as cohort_definition_id_2,

               CASE
                  WHEN t.cohort_definition_id_1 = @targetCohortId THEN cd2.atc_flag
                  ELSE cd.atc_flag
               END as is_atc_2,

               CASE
                  WHEN t.cohort_definition_id_1 = @targetCohortId THEN cd2.short_name
                  ELSE cd.short_name
               END as short_name,
               cosine_similarity,
               atc.atc_4_related,
               atc.atc_3_related,
               CASE
                  WHEN t.cohort_definition_id_1 = @targetCohortId THEN ec.num_persons
                  ELSE ec2.num_persons
               END as num_persons
             from @schema.@cosine_similarity_score  t
             inner join @schema.@cohort_count ec ON ec.cohort_definition_id = t.cohort_definition_id_2 and ec.database_id = t.database_id
             inner join @schema.@cohort_count ec2 ON ec2.cohort_definition_id = t.cohort_definition_id_1 and ec2.database_id = t.database_id
             inner join @schema.@cdm_source_info csi ON csi.database_id = t.database_id
             inner join @schema.@cohort_definition cd ON cd.cohort_definition_id = t.cohort_definition_id_1
             inner join @schema.@cohort_definition cd2 ON cd2.cohort_definition_id = t.cohort_definition_id_2
             left join @schema.@atc_level atc on (t.cohort_definition_id_1 = atc.cohort_definition_id_1 and t.cohort_definition_id_2 = atc.cohort_definition_id_2) or (t.cohort_definition_id_2 = atc.cohort_definition_id_1 and t.cohort_definition_id_1 = atc.cohort_definition_id_2)
             where (t.cohort_definition_id_1 = @targetCohortId or t.cohort_definition_id_2 = @targetCohortId)
             and t.covariate_type = 'average'
           ",
        targetCohortId = targetCohortId)
    }, message = "Loading similarity scores", value = 0.5)

    resultsData %>%
      dplyr::filter(.data$isAtc2 %in% atcSelection)

  })

  # displays target name and counts
  selectedCohortInfo <- function() {

    targetId <- input$selectedExposure
    if (targetId == "")
      return("")
    dbName <- getDatabaseSources() %>%
      filter(databaseId == selectedDatabase()) %>%
      select(cdmSourceAbbreviation) %>%
      pull()

    cohortDefinitions <- getCohortDefinitionsWithCounts()

    target <- cohortDefinitions[cohortDefinitions$cohortDefinitionId == targetId,]
    comparator <- cohortDefinitions[cohortDefinitions$cohortDefinitionId == selectedComparator(),]

    numPersons <- format(round(target$numPersons), big.mark = ",")
    targetText <- paste0(target$shortName, " (", numPersons, " persons)")

    numPersons <- format(round(comparator$numPersons), big.mark = ",")
    comparatorText <- paste0(comparator$shortName, " (", numPersons, " persons)")


    return(div(
      tags$b(paste("Database:", dbName)),
      tags$br(),
      tags$b(paste("Target:", targetText)),
      tags$br(),
      tags$b(paste("Comparator:", comparatorText))
    ))
  }

  #### ---- single-database cosine similarity reactable ---- ####
  output$cosineSimilarityTbl <- reactable::renderReactable({
    sql <- "SELECT covariate_type, cosine_similarity FROM @schema.@cosine_similarity_score
    WHERE database_id = @database_id
    AND cohort_definition_id_1 in (@target, @comparator)
    AND cohort_definition_id_2 in (@target, @comparator)
    "
    detailData <- qns$queryDb(sql,
                              database_id = selectedDatabase(),
                              target = input$selectedExposure,
                              comparator = selectedComparator())


    detailData <- detailData %>%
      dplyr::filter(!covariateType %in% c('Co-occurrence')) %>%
      dplyr::mutate(
        covariateType = factor(
          covariateType,
          levels = c("Demographics",
                     "Presentation",
                     "Medical history",
                     "prior meds",
                     "visit context",
                     "average"))) %>%
      dplyr::arrange(covariateType)

    rt <- reactable::reactable(
      data = detailData,
      columns = list(
        "covariateType" = reactable::colDef(
          name = "Covariate Domain"),
        "cosineSimilarity" = reactable::colDef(
          name = "Cosine Similarity",
          cell = function(value) { sprintf(fmtSim, value) }
        )
      )
    )
    rt

  })


  getAllDbSimFiltered <- shiny::reactive({
    getSimilarityAllDatabases() %>%
      filter(databaseId %in% input$selectedDatabases) %>%
      arrange(databaseId, cdmSourceAbbreviation, desc(cosineSimilarity)) %>%
      group_by(databaseId, cdmSourceAbbreviation, .add = FALSE) %>%
      mutate(cdmSpecificRank = row_number(),
             comparatorsInCdm = n_distinct(cohortDefinitionId2)) %>%
      ungroup() %>%
      mutate(
        cdmSpecificRankStr = paste(
          prettyNum(cdmSpecificRank, big.mark = ","),
          "of",
          prettyNum(comparatorsInCdm, big.mark = ","))) %>%
      group_by(cohortDefinitionId2) %>%
      filter(n() >= input$minNumDatabases) %>%
      ungroup()
  })


  getResSum <- reactive({
    resAll <- getAllDbSimFiltered()

    resSum <- resAll %>%
      ungroup() %>%
      group_by(cohortDefinitionId2, shortName, isAtc2, atc3Related, atc4Related) %>%
      summarise(
        nDatabases = n(),
        avg = mean(ifelse(input$avgOn == "Average similarity score", cosineSimilarity, cdmSpecificRank)),
        .groups = "drop") %>%
      arrange(desc(avg * ifelse(input$avgOn == "Average similarity score", 1, -1))) %>%
      mutate(rank = row_number())

    if (input$avgOn == "Average similarity score") {
      resSum <- resAll %>%
        ungroup() %>%
        group_by(cohortDefinitionId2, shortName, isAtc2, atc3Related, atc4Related) %>%
        summarise(
          nDatabases = n(),
          avg = mean(cosineSimilarity),
          .groups = "drop") %>%
        arrange(desc(avg)) %>%
        mutate(rank = row_number())


    } else if (input$avgOn == "Average source-specific rank") {
      resSum <- resAll %>%
        ungroup() %>%
        group_by(cohortDefinitionId2, shortName, isAtc2, atc3Related, atc4Related) %>%
        summarise(
          nDatabases = n(),
          avg = mean(cdmSpecificRank),
          .groups = "drop") %>%
        arrange(avg) %>%
        mutate(rank = row_number())

    }

    return(resSum)
  })

  #### ---- multi-database cosine similarity reactable ---- ####
  output$multiDatabaseSimTable <- reactable::renderReactable({
    resAll <- getAllDbSimFiltered()
    resSum <- getResSum()
    shiny::withProgress({

      outerOnClick <- sprintf("function(rowInfo, column) {
          // Send the click event to Shiny, which will be available in input$show_details
          // Note that the row index starts at 0 in JavaScript, so we add 1
          if(column.id == 'cohortDefinitionId2'){
            Shiny.setInputValue('%s', { index: rowInfo.values.cohortDefinitionId2 }, { priority: 'event' })
          }
        }", session$ns('show_exclusion')
      )

      rt <- reactable::reactable(
        data = select(resSum, isAtc2, shortName, rank, avg, nDatabases, atc3Related, atc4Related, cohortDefinitionId2),
        details = function(index) {
          cohortId <- resSum$cohortDefinitionId2[index]
          detailData <- resAll[resAll$shortName == resSum$shortName[index], c("databaseId", "cdmSourceAbbreviation", "numPersons", "cosineSimilarity", "cdmSpecificRankStr")]
          detailData <- detailData[order(detailData$cdmSourceAbbreviation),]

          selectionJs <- sprintf("

          function(rowInfo, column) {
          // Send the click event to Shiny, which will be available in input$show_details
          // Note that the row index starts at 0 in JavaScript, so we add 1
          if(column.id == 'databaseId'){
            Shiny.setInputValue('%s', { index: [%f, rowInfo.values.databaseId] }, { priority: 'event' })
          }
        }", session$ns('show_details'), cohortId)

          htmltools::div(
            style = "padding: 1rem",
            reactable::reactable(
              data = detailData,
              columns = list(
                "cdmSourceAbbreviation" = reactable::colDef(
                  name = "Data Source",
                  align = "right",
                  vAlign = "center",
                  headerVAlign = "bottom",
                  minWidth = 125),
                "numPersons" = reactable::colDef(
                  name = "Sample Size",
                  align = "center",
                  cell = function(value) { prettyNum(value, big.mark = ",") },
                  vAlign = "center",
                  headerVAlign = "bottom",
                  minWidth = 125),
                "cosineSimilarity" = reactable::colDef(
                  name = "Cohort Similarity Score",
                  cell = function(value) { sprintf(fmtSim, value) },
                  align = "center",
                  vAlign = "center",
                  headerVAlign = "bottom",
                  minWidth = 125),
                "cdmSpecificRankStr" = reactable::colDef(
                  name = "Source-Specific Rank",
                  align = "right",
                  vAlign = "center",
                  headerVAlign = "bottom",
                  minWidth = 125),
                "databaseId" = reactable::colDef(
                  name = "",
                  sortable = FALSE,
                  filterable = FALSE,
                  cell = function() htmltools::tags$button("View Covariates")
                )
              ),
              onClick = reactable::JS(selectionJs),
              outlined = TRUE)
          )
        },
        columns = list(
          "cohortDefinitionId2" = reactable::colDef(
            name = "",
            sortable = FALSE,
            filterable = FALSE,
            cell = function() htmltools::tags$button("View exclusion covariates")
          ),
          "isAtc2" = reactable::colDef(
            name = "Type",
            cell = function(value) { ifelse(value == 1, "ATC Class", "RxNorm Ingredient") },
            align = "right",
            vAlign = "center",
            headerVAlign = "bottom",
            minWidth = 125),
          "rank" = reactable::colDef(
            name = "Overall Rank",
            cell = function(value) { prettyNum(value, big.mark = ",") },
            align = "center",
            vAlign = "center",
            headerVAlign = "bottom",
            minWidth = 125),
          "avg" = reactable::colDef(
            name = stringr::str_to_title(input$avgOn),
            cell = function(value) { if (input$avgOn == "Average similarity score") { sprintf(fmtSim, value) } else { format(round(value, 1), nsmall = 1, big.mark = ",") } },
            align = "center",
            vAlign = "center",
            headerVAlign = "bottom",
            minWidth = 125),
          "shortName" = reactable::colDef(
            name = "Name",
            cell = function(value) { ifelse(substr(value, 1, 6) == "RxNorm", gsub("RxNorm - ", "", value), gsub("ATC - ", "", value)) },
            align = "left",
            vAlign = "center",
            headerVAlign = "bottom",
            minWidth = 125),
          "nDatabases" = reactable::colDef(
            name = "Number of Databases",
            cell = function(value) { prettyNum(value, big.mark = ",") },
            align = "center",
            vAlign = "center",
            headerVAlign = "bottom",
            minWidth = 125),
          "atc3Related" = reactable::colDef(
            name = "At Level 3",
            cell = function(value) ifelse(is.na(value) | value == 0, "No", "Yes"),
            align = "center",
            vAlign = "center",
            headerVAlign = "bottom",
            filterable = TRUE),
          "atc4Related" = reactable::colDef(
            name = "At Level 4",
            cell = function(value) ifelse(is.na(value) | value == 0, "No", "Yes"),
            align = "center",
            vAlign = "center",
            headerVAlign = "bottom",
            filterable = TRUE)
        ),
        searchable = TRUE,
        columnGroups = list(
          reactable::colGroup(
            "Comparator",
            c("shortName", "isAtc2")),
          reactable::colGroup(
            "In ATC Class with Target",
            c("atc3Related", "atc4Related"))),
        fullWidth = TRUE,
        showPageSizeOptions = TRUE,
        pageSizeOptions = c(5, 10, 20, 50, 100, 1000),
        striped = TRUE,
        highlight = TRUE,
        compact = TRUE,
        defaultSorted = list(rank = "asc"),
        theme = reactable::reactableTheme(
          borderColor = "#dfe2e5",
          stripedColor = "#f6f8fa",
          highlightColor = "#eab676",
          cellPadding = "8px 12px",
          searchInputStyle = list(width = "100%")),
        onClick = reactable::JS(outerOnClick),
        showSortIcon = TRUE)
    }, message = "Rendering results", value = 0.7)
    return(rt)
  })

  output$selectedComparator <- shiny::reactive({
    selection <- reactable::getReactableState("cosineSimilarityTbl", name = "selected")
    return(!is.null(selection))
  })

  shiny::observeEvent(input$show_details, {

    tagBox <- div(
      selectedCohortInfo(),
      shinycssloaders::withSpinner(reactable::reactableOutput("cosineSimilarityTbl"))
    )

    showModal(
      modalDialog(
        title = "Covariate data",
        shiny::tagList(
          tagBox,
          modalUi()
        ),
        size = "l",
        easyClose = FALSE,
        footer = shiny::tagList(
          shiny::actionButton("closeModal", "Close")
        )
      ))
  })


  shiny::observeEvent(input$show_exclusion, {
    shiny::showModal(
      modalDialog(
        title = "Covariate exclusion",
        shiny::tagList(
          h4(strong("Find exclusion covariates")),
          p("Covariates that occur on the same day as the exposure start day.",
            "Those with high prevalance are found on the same day as an exposure will likely introduce bias in propensity score matching"),
          p("Adjust sliders to select prevalance of allowed covariates (i.e. display those outside of these ranges)"),
          shiny::fluidRow(
            shiny::column(width = 6,
                          shiny::sliderInput("targetPrevalanceEx",
                                             label = "target prevalance range",
                                             min = 0,
                                             max = 1.0,
                                             step = 0.01,
                                             value = c(0.05, 0.95))
            ),
            shiny::column(width = 6,
                          shiny::sliderInput("comparatorPrevalanceEx",
                                             label = "comparator prevalance range",
                                             min = 0,
                                             max = 1.0,
                                             step = 0.01,
                                             value = c(0.2, 0.8))
            )
          ),
          shinycssloaders::withSpinner(reactable::reactableOutput("covTableCoOccurrence")),
          shiny::div(
            style = "text-align:right;",
            withTooltip(shiny::tags$button("Download",
                                           onclick = paste0("Reactable.downloadDataCSV('covTableCoOccurrence')")),
                        tooltip = "Note, will not download live values filtered in table, groupings, or any graphical/stylstic elements")
          )
        ),
        size = "l",
        easyClose = FALSE,
        footer = shiny::tagList(
          shiny::actionButton("closeModal", "Close")
        )
      )
    )
  })

  shiny::observeEvent(input$closeModal, {
    shiny::removeModal()
  })

  shiny::outputOptions(output,
                       "selectedComparator",
                       suspendWhenHidden = FALSE)

  #### ---- step-function plot of cosine similarity by rank ---- ####
  output$stepPlot <- plotly::renderPlotly({

    resWide <- getDbSimilarity() %>%
      dplyr::mutate(var = NA) %>%
      dplyr::mutate(
        var = ifelse(covariateType == "average", "cohortSimScore", var),
        var = ifelse(covariateType == "Demographics", "csDemo", var),
        var = ifelse(covariateType == "Presentation", "csPres", var),
        var = ifelse(covariateType == "Medical history", "csMhis", var),
        var = ifelse(covariateType == "prior meds", "csPmed", var),
        var = ifelse(covariateType == "visit context", "csVisc", var)) %>%
      dplyr::mutate(
        var = factor(var, levels = c("cohortSimScore", "csDemo", "csPres", "csMhis", "csPmed", "csVisc"))) %>%
      tidyr::pivot_wider(
        id_cols = c("cohortDefinitionId2", "isAtc2", "shortName", "numPersons", "cdmSourceAbbreviation"),
        names_from = var,
        values_fill = NA,
        values_from = cosineSimilarity) %>%
      ungroup()

    if (!"csVisc" %in% colnames(resWide)) resWide$csVisc <- NA

    resWide <- resWide %>%
      dplyr::arrange(desc(cohortSimScore)) %>%
      dplyr::mutate(
        rank = row_number(),
        tooltip = paste0(
          "<extra></extra>",
          "<b>",
          stringr::str_wrap(string = shortName, width = 20, indent = 1, exdent = 1),
          "</b>\n",
          "Cohort similarity score: ", sprintf(fmtSim, cohortSimScore), "\n",
          "Rank:", prettyNum(row_number(), big.mark = ","), " of ", prettyNum(nrow(.), big.mark = ","), "\n",
          "Domain-specific cosine similarity:\n",
          "\t<i> Demographics: </i>", sprintf(fmtSim, csDemo), "\n",
          "\t<i> Presentation: </i>", sprintf(fmtSim, csPres), "\n",
          "\t<i> Medical history: </i>", sprintf(fmtSim, csMhis), "\n",
          "\t<i> Prior medications: </i>", sprintf(fmtSim, csPmed), "\n",
          "\t<i> Visit context: </i>", ifelse(is.na(csVisc), "n/a", sprintf(fmtSim, csVisc))))

    plotly::plot_ly(
      data = resWide,
      x = ~rank,
      y = ~cohortSimScore,
      color = ~cdmSourceAbbreviation,
      type = "scatter",
      mode = "lines",
      text = ~tooltip,
      hovertemplate = "%{text}") %>%
      plotly::layout(
        hovermode = "x unified",
        xaxis = list(title = "Rank"),
        yaxis = list(title = "Cohort Similarity Score"),
        legend = list(orientation = 'h', y = -0.5))

  })

  ##### ---- function to get covariate data for a given comparison ---- ####
  getCovData <- shiny::reactive({
    validate(need(input$selectedExposure, "must select exposure"),
             need(selectedComparator(), "must select comparator"))

    shiny::withProgress({
      covData <- qns$queryDb(
        sql = "with means as (
              	select
              		@cohortDefinitionId1 as cohort_definition_id_1,
              		@cohortDefinitionId2 as cohort_definition_id_2,
              		case
              			when c1.covariate_type is null then c2.covariate_type
              			when c2.covariate_type is null then c1.covariate_type
              			else c1.covariate_type
              		end as covariate_type,
              		case
              			when c1.covariate_id is null then c2.covariate_id
              			when c2.covariate_id is null then c1.covariate_id
              			else c1.covariate_id
              		end as covariate_id,
              		case
              			when c1.covariate_name is null then c2.covariate_name
              			when c2.covariate_name is null then c1.covariate_name
              			else c1.covariate_name
              		end as covariate_short_name,
              		case
              			when c1.covariate_mean is null then 0.0
              			else c1.covariate_mean
              		end as mean_1,
              		case
              			when c2.covariate_mean is null then 0.0
              			else c2.covariate_mean
              		end as mean_2
              	from (
              	  select t.*, covd.covariate_name, covd.covariate_type from @schema.@covariate_mean t
              	  inner join @schema.@covariate_definition covd on covd.covariate_id = t.covariate_id
              	  where t.cohort_definition_id = @cohortDefinitionId1
              	  and t.database_id = @database_id
            	  ) as c1
              	full join (
              	  select t.*, covd.covariate_name, covd.covariate_type from @schema.@covariate_mean t
              	  inner join @schema.@covariate_definition covd on covd.covariate_id = t.covariate_id
              	   where t.cohort_definition_id = @cohortDefinitionId2
              	   and t.database_id = @database_id
              	 ) as c2
              on c1.covariate_id = c2.covariate_id)
              select
              	m.*,
              	case
              		when m.mean_1 = m.mean_2 then 0.0
              		when m.mean_1 = 0.0 and m.mean_2 = 1.0 then null
              		when m.mean_1 = 1.0 and m.mean_2 = 0.0 then null
              		else (mean_1 - mean_2) / (sqrt((mean_1 * (1 - mean_1) + mean_2 * (1 - mean_2)) / 2))
              	end as std_diff,
              c1.num_persons as n_1,
              c2.num_persons as n_2
              from means as m
              join @schema.@cohort_count as c1
              	on m.cohort_definition_id_1 = c1.cohort_definition_id and c1.database_id = @database_id
              join @schema.@cohort_count as c2
              	on m.cohort_definition_id_2 = c2.cohort_definition_id and c2.database_id = @database_id
              ;",
        database_id = selectedDatabase(),
        cohortDefinitionId1 = input$selectedExposure,
        cohortDefinitionId2 = selectedComparator())
    }, message = "Loading covariate data")
    # return data
    covData

  })

  #### ---- scatterplot of covariate prevalence ---- ####
  output$scatterPlot <- plotly::renderPlotly({

    shiny::validate(need(input$selectedExposure, 'must select exposure'),
                    need(selectedComparator(), 'must select comparator'))

    plot <- getCovData() %>%
      mutate(
        covariateShortName = gsub("Condition in <=30d prior:", "", covariateShortName),
        covariateShortName = gsub("Condition in >30d prior:", "", covariateShortName),
        covariateShortName = gsub("Drug with start >30d prior:", "", covariateShortName),
        covariateShortName = stringr::str_to_sentence(gsub("<=30d prior|Visit:", "", covariateShortName))) %>%
      mutate(
        type = NA,
        type = ifelse(covariateType == "Demographics", "Demographics", type),
        type = ifelse(covariateType == "Presentation", "Presentation", type),
        type = ifelse(covariateType == "Medical history", "Medical History", type),
        type = ifelse(covariateType == "prior meds", "Prior Medications", type),
        type = ifelse(covariateType == "visit context", "Visit Context", type),
        type = factor(type, levels = c("Demographics", "Presentation", "Medical History", "Prior Medications", "Visit Context")),
        tooltip = paste0(
          "<b>",
          stringr::str_wrap(string = covariateShortName, width = 20, indent = 1, exdent = 1),
          "</b>\n",
          "Target: ", ifelse(mean1 < 0.01, "<1%", scales::percent(mean1, accuracy = 0.1)), "\n",
          "Comparator: ", ifelse(mean2 < 0.01, "<1%", scales::percent(mean2, accuracy = 0.1)), "\n",
          "Std. Diff.: ", ifelse(mean1 < 0.01 | mean2 < 0.01,
                                 ifelse(mean1 < 0.01, paste0("(\u2265) ", sprintf(fmtSmd, stdDiff)), paste0("(\u2264) ", sprintf(fmtSmd, stdDiff))),
                                 sprintf(fmtSmd, stdDiff))
        )) %>%
      plotly::plot_ly(
        type = 'scatter',
        mode = 'markers',
        x = ~mean1,
        y = ~mean2,
        color = ~type,
        text = ~tooltip,
        marker = list(opacity = 0.7),
        hovertemplate = "%{text}"
      ) %>%
      plotly::layout(
        xaxis = list(title = "Prevalence in\nTarget Cohort", tickformat = ".0%"),
        yaxis = list(title = "Prevalence in\nComparator Cohort", tickformat = ".0%"),
        legend = list(orientation = 'h', y = -0.5),
        shapes = list(list(
          type = "line",
          x0 = 0,
          x1 = ~max(mean1, mean2),
          xref = "x",
          y0 = 0,
          y1 = ~max(mean1, mean2),
          yref = "y",
          line = list(color = "black", dash = "dot")
        ))
      )
  })

  #### ---- plot of std. diffs. ---- ####
  output$smdPlot <- plotly::renderPlotly({
    shiny::validate(need(input$selectedExposure, 'must select exposure'),
                    need(selectedComparator(), 'must select comparator'))

    vline <- function(x = 0, color = "black") {
      list(
        type = "line",
        y0 = 0,
        y1 = 1,
        yref = "paper",
        x0 = x,
        x1 = x,
        line = list(color = color, dash = "dot")
      )
    }

    plot <- getCovData() %>%
      mutate(
        covariateShortName = gsub("Condition in <=30d prior:", "", covariateShortName),
        covariateShortName = gsub("Condition in >30d prior:", "", covariateShortName),
        covariateShortName = gsub("Drug with start >30d prior:", "", covariateShortName),
        covariateShortName = stringr::str_to_sentence(gsub("<=30d prior|Visit:", "", covariateShortName))) %>%
      mutate(
        type = NA,
        type = ifelse(covariateType == "Demographics", "Demographics", type),
        type = ifelse(covariateType == "Presentation", "Presentation", type),
        type = ifelse(covariateType == "Medical history", "Medical History", type),
        type = ifelse(covariateType == "prior meds", "Prior Medications", type),
        type = ifelse(covariateType == "visit context", "Visit Context", type),
        type = factor(type, levels = (c("Demographics", "Presentation", "Medical History", "Prior Medications", "Visit Context"))),
        tooltip = paste0(
          "<b>",
          stringr::str_wrap(string = covariateShortName, width = 20, indent = 1, exdent = 1),
          "</b>\n",
          "Target: ", ifelse(mean1 < 0.01, "<1%", scales::percent(mean1, accuracy = 0.1)), "\n",
          "Comparator: ", ifelse(mean2 < 0.01, "<1%", scales::percent(mean2, accuracy = 0.1)), "\n",
          "Std. Diff.: ", ifelse(mean1 < 0.01 | mean2 < 0.01,
                                 ifelse(mean1 < 0.01, paste0("(\u2265) ", sprintf(fmtSmd, stdDiff)), paste0("(\u2264) ", sprintf(fmtSmd, stdDiff))),
                                 sprintf(fmtSmd, stdDiff))
        )) %>%
      plotly::plot_ly(
        hovertemplate = "%{text}"
      ) %>%
      plotly::add_markers(
        x = ~stdDiff,
        y = ~jitter(as.numeric(type)),
        color = ~type,
        marker = list(opacity = 0.7),
        text = ~tooltip
      ) %>%
      plotly::layout(
        xaxis = list(title = "Standardized Difference"),
        yaxis = list(title = "", showticklabels = FALSE),
        shapes = list(vline(-0.1), vline(0.1)),
        legend = list(orientation = 'h', y = -0.5))

  })

  inBalanceString <- function(covData) {
    inBalanceCount <- covData %>%
      filter(abs(stdDiff) < 0.1) %>%
      count() %>%
      pull()

    percentBalanced <- round(inBalanceCount / nrow(covData) * 100, 1)
    paste(inBalanceCount, " of", nrow(covData), "covariates", paste0("(", percentBalanced, "%)"),
          "have absolute standardized difference less than 0.1")
  }

  output$covTableDemoBalance <- shiny::renderText({
    covData <- getCovData() %>% filter(covariateType == "Demographics")
    inBalanceString(covData)
  })

  #### ---- "table 1": demographics (default to unsorted) ---- ####
  output$covTableDemo <- reactable::renderReactable({
    cohortDefinitions <- getCohortDefinitionsWithCounts()
    # get data
    covData <- getCovData()

    # create column names with cohort sample sizes
    targetName <- paste0(
      cohortDefinitions$shortName[cohortDefinitions$cohortDefinitionId == input$selectedExposure],
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
      filter(covariateType == "Demographics") %>%
      arrange(desc(covariateShortName)) %>%
      select(covariateShortName, mean1, mean2, stdDiff) %>%
      mutate(covariateShortName = stringr::str_to_sentence(covariateShortName))

    # table code
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
  })

  output$covTablePresBalance <- shiny::renderText({
    covData <- getCovData() %>% filter(covariateType == "Presentation")
    inBalanceString(covData)
  })

  #### ---- "table 1": presentation (default to sort by abs. std. diff.) ---- ####
  output$covTablePres <- reactable::renderReactable({
    cohortDefinitions <- getCohortDefinitionsWithCounts()
    # get data
    covData <- getCovData()

    # create column names with cohort sample sizes
    targetName <- paste0(
      cohortDefinitions$shortName[cohortDefinitions$cohortDefinitionId == input$selectedExposure],
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
      filter(covariateType == "Presentation") %>%
      arrange(desc(abs(stdDiff))) %>%
      select(covariateShortName, mean1, mean2, stdDiff) %>%
      mutate(covariateShortName = gsub("Condition in <=30d prior:", "", covariateShortName))

    # table code
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

  })

  output$covTableMhistBalance <- shiny::renderText({
    covData <- getCovData() %>% filter(covariateType == "Medical history")
    inBalanceString(covData)
  })

  #### ---- "table 1": medical history (default to sort by abs. std. diff.) ---- ####
  output$covTableMhist <- reactable::renderReactable({
    cohortDefinitions <- getCohortDefinitionsWithCounts()
    # get data
    covData <- getCovData()

    # create column names with cohort sample sizes
    targetName <- paste0(
      cohortDefinitions$shortName[cohortDefinitions$cohortDefinitionId == input$selectedExposure],
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
      filter(covariateType == "Medical history") %>%
      arrange(desc(abs(stdDiff))) %>%
      select(covariateShortName, mean1, mean2, stdDiff) %>%
      mutate(covariateShortName = gsub("Condition in >30d prior:", "", covariateShortName))

    # table code
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

  })

  output$covTablePmedsBalance <- shiny::renderText({
    covData <- getCovData() %>% filter(covariateType == "prior meds")
    inBalanceString(covData)
  })

  #### ---- "table 1": prior meds (default to sort by abs. std. diff.) ---- ####
  output$covTablePmeds <- reactable::renderReactable({
    cohortDefinitions <- getCohortDefinitionsWithCounts()
    # get data
    covData <- getCovData()

    # create column names with cohort sample sizes
    targetName <- paste0(
      cohortDefinitions$shortName[cohortDefinitions$cohortDefinitionId == input$selectedExposure],
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
      filter(covariateType == "prior meds") %>%
      arrange(desc(abs(stdDiff))) %>%
      select(covariateShortName, mean1, mean2, stdDiff) %>%
      mutate(covariateShortName = gsub("Drug with start >30d prior:", "", covariateShortName))

    # table code
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

  })

  output$covTableVisitBalance <- shiny::renderText({
    covData <- getCovData() %>% filter(covariateType == "visit context")
    inBalanceString(covData)
  })


  getCoOccurrenceData <- shiny::reactive({
    validate(need(input$selectedExposure, "must select exposure"),
             need(input$show_exclusion$index, "must select comparator"))

    covData <- qns$queryDb(
      sql = "
            with means as (
              	select
              	    c1.database_id,
              		@cohortDefinitionId1 as cohort_definition_id_1,
              		@cohortDefinitionId2 as cohort_definition_id_2,
              		case
              			when c1.covariate_type is null then c2.covariate_type
              			when c2.covariate_type is null then c1.covariate_type
              			else c1.covariate_type
              		end as covariate_type,
              		case
              			when c1.covariate_id is null then c2.covariate_id
              			when c2.covariate_id is null then c1.covariate_id
              			else c1.covariate_id
              		end as covariate_id,
              		case
              			when c1.covariate_name is null then c2.covariate_name
              			when c2.covariate_name is null then c1.covariate_name
              			else c1.covariate_name
              		end as covariate_short_name,
              		case
              			when c1.covariate_mean is null then 0.0
              			else c1.covariate_mean
              		end as mean_1,
              		case
              			when c2.covariate_mean is null then 0.0
              			else c2.covariate_mean
              		end as mean_2
              	from (
              	  select t.*, covd.covariate_name, covd.covariate_type from @schema.@covariate_mean t
              	  inner join @schema.@covariate_definition covd on covd.covariate_id = t.covariate_id
              	  where t.cohort_definition_id = @cohortDefinitionId1
              	  and t.database_id IN (@database_ids)
            	  ) as c1
              	full join (
              	  select t.*, covd.covariate_name, covd.covariate_type from @schema.@covariate_mean t
              	  inner join @schema.@covariate_definition covd on covd.covariate_id = t.covariate_id
              	   where t.cohort_definition_id = @cohortDefinitionId2
              	   and t.database_id IN (@database_ids)
              	 ) as c2
              on c1.covariate_id = c2.covariate_id AND c1.database_id = c2.database_id
              WHERE c1.covariate_type IN (NULL, 'Co-occurrence') AND c2.covariate_type IN (NULL, 'Co-occurrence')
            )

            select
              m.*,
              d.cdm_source_abbreviation,
              case
                  when m.mean_1 = m.mean_2 then 0.0
                  when m.mean_1 = 0.0 and m.mean_2 = 1.0 then null
                  when m.mean_1 = 1.0 and m.mean_2 = 0.0 then null
                  else (mean_1 - mean_2) / (sqrt((mean_1 * (1 - mean_1) + mean_2 * (1 - mean_2)) / 2))
              end as std_diff,
            c1.num_persons as n_1,
            c2.num_persons as n_2
            from means as m
            join @schema.@cohort_count as c1
            on m.cohort_definition_id_1 = c1.cohort_definition_id and c1.database_id = m.database_id
            join @schema.@cohort_count as c2
              on m.cohort_definition_id_2 = c2.cohort_definition_id and c2.database_id = m.database_id
            inner join @schema.@cdm_source_info as d on d.database_id = m.database_id

            WHERE (m.mean_1 > @targetPrevMin AND m.mean_2 > @comparatorPrevMin)
            OR (m.mean_1 < @targetPrevMax AND m.mean_2 < @comparatorPrevMax)
              ;",
      database_ids = input$selectedDatabases,
      comparatorPrevMin = input$comparatorPrevalanceEx[1],
      comparatorPrevMax = input$comparatorPrevalanceEx[2],
      targetPrevMin = input$targetPrevalanceEx[1],
      targetPrevMax = input$targetPrevalanceEx[2],
      cohortDefinitionId1 = input$selectedExposure,
      cohortDefinitionId2 = input$show_exclusion$index)
    # return data
    covData
  })

  output$covTableCoOccurrence <- reactable::renderReactable({
    cohortDefinitions <- getCohortDefinitionsWithCounts()
    # get data
    covData <- getCoOccurrenceData()
    # create column names with cohort sample sizes
    targetName <- paste0(
      cohortDefinitions$shortName[cohortDefinitions$cohortDefinitionId == input$show_exclusion$index],
      " (n = ",
      prettyNum(first(covData$n1), big.mark = ","),
      ")")

    comparatorName <- paste0(
      cohortDefinitions$shortName[cohortDefinitions$cohortDefinitionId == input$selected],
      " (n = ",
      prettyNum(first(covData$n2), big.mark = ","),
      ")")

    # subset data and select relevant columns
    tableData <- covData %>%
      filter() %>%
      arrange(desc(abs(stdDiff))) %>%
      select(covariateId, cdmSourceAbbreviation, covariateShortName, mean1, mean2, stdDiff) %>%
      mutate(covariateShortName = gsub("concept co-occurrence:", "", covariateShortName)) %>%
      mutate(conceptId = abs(covariateId)) %>%
      select(-covariateId)

    # table code
    reactable::reactable(
      data = tableData,
      columns = list(
        "cdmSourceAbbreviation" = reactable::colDef(name = "Data Source", align = "right", vAlign = "bottom"),
        "conceptId" = reactable::colDef(name = "Concept Id", align = "right", vAlign = "bottom"),
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

  })
  #### ---- "table 1": visit context (default to unsorted) ---- ####
  output$covTableVisit <- reactable::renderReactable({
    cohortDefinitions <- getCohortDefinitionsWithCounts()
    # get data
    covData <- getCovData()

    shiny::withProgress({
      # create column names with cohort sample sizes
      targetName <- paste0(
        cohortDefinitions$shortName[cohortDefinitions$cohortDefinitionId == input$selectedExposure],
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
        filter(covariateType == "visit context") %>%
        arrange(covariateShortName) %>%
        select(covariateShortName, mean1, mean2, stdDiff) %>%
        mutate(covariateShortName = stringr::str_to_sentence(gsub("<=30d prior|Visit:", "", covariateShortName)))

      # table code
      rt <- reactable::reactable(
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

    },
      message = "Rendering tables",
      value = 0.7)
    rt
  })

  output$dataSources <- reactable::renderReactable({
    dataSourceData <- qns$queryDb(
      sql = "select
              cdm_source_abbreviation,
              cdm_holder,
              source_description,
              cdm_version,
              vocabulary_version,
              source_release_date
      from @schema.@cdm_source_info t")

    colnames(dataSourceData) <- SqlRender::camelCaseToTitleCase(colnames(dataSourceData))
    reactable::reactable(
      data = dataSourceData,
      columns = list(
        "Source Description" = reactable::colDef(
          minWidth = 300)),
      defaultPageSize = 5)
  })


  observeEvent(input$showRankings, {
    shiny::showModal(shiny::modalDialog(title = "Comparator Ranks",
                                        p(""),
                                        shinycssloaders::withSpinner(plotly::plotlyOutput("stepPlot"))))
  })
})
