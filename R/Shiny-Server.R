# No library() calls at the top!

comparatorSelectionAppModuleServer <- function(id, qns) {

  # decimal formatters
  fmtSim <- "%.3f"
  fmtSmd <- "%.2f"
  shiny::moduleServer(id, function(input, output, session) {
    ns <- session$ns
    selectedDatabase <- shiny::reactive({
      input$show_details$index[2]
    })

    selectedDatabases <- shiny::reactive({
      input$selectedDatabases
    })

    cohortTable <- shiny::reactive({
      getCohortDefinitions(qns)
    })

    selectedComparator <- shiny::reactive({
      input$show_details$index[1]
    })

    selectedExposure <- shiny::reactive(input$selectedExposure)

    domainWeights <- shiny::reactive({
      weights <- c(
        Demographics = round((input$userWeightDemo / 100), 2),
        Presentation = round((input$userWeightPres / 100), 2),
        `Medical history` = round((input$userWeightHist / 100), 2),
        `prior meds` = round((input$userWeightMeds / 100), 2),
        `visit context` = round((input$userWeightVisit / 100), 2)
      )
      if (isTRUE(input$useWeights && sum(weights) > 0)) {
        round(weights / sum(weights), 2)
      } else {
        rep(1 / 5, 5)
      }
    })

    output$weightSummary <- shiny::renderTable({
      if (!input$useWeights) return(NULL)

      weights <- domainWeights()
      data.frame(
        Domain = names(weights),
        `Scaled Weight (%)` = round(weights * 100, 2),
        check.names = FALSE
      )
    }, striped = TRUE, bordered = TRUE)

    getCohortDefinitionsWithDbCounts <- shiny::reactive({
      dbSel <- selectedDatabase()
      if (is.null(dbSel) || dbSel == "")
        return(data.frame())

      getCohortDefinitionsTable(qns, databaseId = dbSel)
    })

    databaseSources <- shiny::reactive({
      getDatabaseSources(qns)
    })

    getExposureTags <- shiny::reactive({
      getCohortTags(qns)
    })

    shiny::observe({
      shiny::withProgress({
        dbSources <- databaseSources()
        dbChoices <- dbSources$databaseId
        names(dbChoices) <- dbSources$cdmSourceAbbreviation
        dbChoices <- dbChoices[order(names(dbChoices))]

        shiny::updateSelectizeInput(
          session,
          "selectedDatabase",
          choices = dbChoices,
          selected = dbChoices[which(names(dbChoices) == "IBM CCAE")],
          server = TRUE)
      }, message = "Loading database sources")
    })

    shiny::observe({
      shiny::withProgress({
        dbSources <- databaseSources()
        dbChoices <- dbSources$databaseId
        names(dbChoices) <- dbSources$cdmSourceAbbreviation
        dbChoices <- dbChoices[order(names(dbChoices))]

        shiny::updateSelectInput(
          session,
          "selectedDatabases",
          choices = dbChoices,
          selected = dbChoices
        )
      }, message = "Loading database sources")
    })

    shiny::observe({
      shiny::withProgress({
        dbSources <- databaseSources()
        dbChoices <- dbSources$databaseId

        shiny::updateSliderInput(
          session,
          inputId = "minNumDatabases",
          min = 1,
          max = length(dbChoices),
          step = 1)
      }, message = "Loading database sources")
    })

    shiny::observe({

      shiny::withProgress({
        exposureTags <- getExposureTags()
        shiny::updateSelectizeInput(
            session,
            "selectedExposureGroups",
            choices = exposureTags$tag,
            selected = exposureTags$tag[1],
            server = TRUE)
        }, message = "Getting exposure tags"
      )
    })


    shiny::observeEvent(input$selectedExposureGroups, {
      shiny::withProgress({
        cohortDefinitions <- getCohortsByTag(qns, input$selectedExposureGroups)
        if (nrow(cohortDefinitions)) {
          exposureSelection <- cohortDefinitions$cohortDefinitionId
          names(exposureSelection) <- cohortDefinitions$shortName
          shiny::updateSelectizeInput(
            session,
            "selectedExposure",
            choices = exposureSelection,
            selected = 8826,
            server = TRUE)
        }
      }, message = "Loading cohort definitions")
    })


    # shiny::observe({
    #   shiny::withProgress({
    #
    #     cohortDefinitions <- dplyr::filter(cohortTable(), .data$isAtc %in% getTargetClassSelection())
    #
    #     if (nrow(cohortDefinitions)) {
    #       exposureSelection <- cohortDefinitions$cohortDefinitionId
    #       names(exposureSelection) <- cohortDefinitions$shortName
    #       shiny::updateSelectizeInput(
    #         session,
    #         "selectedExposure",
    #         choices = exposureSelection,
    #         server = TRUE)
    #     }
    #   }, message = "Loading cohort definitions")
    # })


    getTargetClassSelection <- shiny::reactive({
      atcSelection <- as.integer(input$selectedComparatorTypes)
      if (length(atcSelection) == 0) {
        atcSelection <- c(0, 1)
      }
      return(atcSelection)
    })

    getDbSimilarity <- shiny::reactive({
      targetCohortId <- input$selectedExposure
      shiny::validate(shiny::need(input$selectedExposure, "must select exposure"))

      shiny::withProgress({
        atcSelection <- getTargetClassSelection()
        resultsData <- getDatabaseSimilarityScores(qns,
                                                   targetCohortId = targetCohortId,
                                                   databaseIds = input$selectedDatabases)
      }, message = "Loading similarity scores", value = 0.5)

      dplyr::filter(resultsData, .data$isAtc2 %in% atcSelection)
    })

    getSimilarityAllDatabases <- shiny::eventReactive(input$getResults, {
      targetCohortId <- as.numeric(input$selectedExposure)
      shiny::validate(shiny::need(targetCohortId, "must select exposure"))

      weights <- shiny::isolate(domainWeights())

      shiny::withProgress({
        atcSelection <- getTargetClassSelection()
        resultsData <- getCohortSimilarityScores(qns, targetCohortId, weights)
      }, message = "Loading similarity scores", value = 0.5)

      dplyr::filter(resultsData, .data$isAtc2 %in% atcSelection)
    })

    selectedCohortInfo <- function() {
      targetId <- input$selectedExposure
      if (targetId == "")
        return("")

      dbName <- dplyr::filter(databaseSources(), .data$databaseId == selectedDatabase()) |>
        dplyr::select("cdmSourceAbbreviation") |>
        dplyr::pull()

      cohortDefinitions <- getCohortDefinitionsWithDbCounts()

      target <- cohortDefinitions[cohortDefinitions$cohortDefinitionId == selectedExposure(),]
      comparator <- cohortDefinitions[cohortDefinitions$cohortDefinitionId == selectedComparator(),]

      numPersons <- format(round(target$numPersons), big.mark = ",")
      targetText <- paste0(target$shortName, " (", numPersons, " persons)")

      numPersons <- format(round(comparator$numPersons), big.mark = ",")
      comparatorText <- paste0(comparator$shortName, " (", numPersons, " persons)")

      shiny::fluidRow(
        shiny::column(width = 4, tags$b(paste("Database:", dbName))),
        shiny::column(width = 4, tags$b(paste("Target:", targetText))),
        shiny::column(width = 4, tags$b(paste("Comparator:", comparatorText)))
      )
    }

    output$cosineSimilarityTbl <- reactable::renderReactable({
      weights <- if (isTRUE(input$useWeights)) domainWeights() else NULL

      getDbCosineSimilarityTable(qns,
                                 databaseId = selectedDatabase(),
                                 targetCohortId = input$selectedExposure,
                                 comparatorCohortId = selectedComparator(),
                                 weights = weights,
                                 returnReactable = TRUE)
    })

    getAllDbSimFiltered <- shiny::reactive({
      getSimilarityAllDatabases() |>
        dplyr::filter(.data$databaseId %in% input$selectedDatabases) |>
        dplyr::arrange(.data$databaseId, .data$cdmSourceAbbreviation, dplyr::desc(.data$cosineSimilarity)) |>
        dplyr::group_by(.data$databaseId, .data$cdmSourceAbbreviation, .add = FALSE) |>
        dplyr::mutate(
          cdmSpecificRank = dplyr::row_number(),
          comparatorsInCdm = dplyr::n_distinct(.data$cohortDefinitionId2)
        ) |>
        dplyr::ungroup() |>
        dplyr::mutate(
          cdmSpecificRankStr = paste(
            prettyNum(.data$cdmSpecificRank, big.mark = ","),
            "of",
            prettyNum(.data$comparatorsInCdm, big.mark = ","))
        ) |>
        dplyr::group_by(.data$cohortDefinitionId2) |>
        dplyr::filter(dplyr::n() >= input$minNumDatabases) |>
        dplyr::ungroup()
    })

    getResSum <- shiny::reactive({
      resAll <- getAllDbSimFiltered()

      resSum <- resAll |>
        dplyr::ungroup() |>
        dplyr::group_by(.data$cohortDefinitionId2, .data$shortName, .data$isAtc2, .data$atc3Related, .data$atc4Related) |>
        dplyr::summarise(
          nDatabases = dplyr::n(),
          avg = mean(ifelse(input$avgOn == "Average similarity score", .data$cosineSimilarity, .data$cdmSpecificRank)),
          .groups = "drop") |>
        dplyr::arrange(dplyr::desc(.data$avg * ifelse(input$avgOn == "Average similarity score", 1, -1))) |>
        dplyr::mutate(rank = dplyr::row_number())

      if (input$avgOn == "Average similarity score") {
        resSum <- resAll |>
          dplyr::ungroup() |>
          dplyr::group_by(.data$cohortDefinitionId2, .data$shortName, .data$isAtc2, .data$atc3Related, .data$atc4Related) |>
          dplyr::summarise(
            nDatabases = dplyr::n(),
            avg = mean(.data$cosineSimilarity),
            .groups = "drop") |>
          dplyr::arrange(dplyr::desc(.data$avg)) |>
          dplyr::mutate(rank = dplyr::row_number())
      } else if (input$avgOn == "Average source-specific rank") {
        resSum <- resAll |>
          dplyr::ungroup() |>
          dplyr::group_by(.data$cohortDefinitionId2, .data$shortName, .data$isAtc2, .data$atc3Related, .data$atc4Related) |>
          dplyr::summarise(
            nDatabases = dplyr::n(),
            avg = mean(.data$cdmSpecificRank),
            .groups = "drop") |>
          dplyr::arrange(.data$avg) |>
          dplyr::mutate(rank = dplyr::row_number())
      }

      return(resSum)
    })

    output$multiDatabaseSimTable <- reactable::renderReactable({
      resAll <- getAllDbSimFiltered()
      resSum <- getResSum()
      shiny::withProgress({

        outerOnClick <- sprintf("function(rowInfo, column) {
          if(column.id == 'cohortDefinitionId2'){
            Shiny.setInputValue('%s', { index: rowInfo.values.cohortDefinitionId2 }, { priority: 'event' })
          }
        }", session$ns('show_exclusion')
        )

        rt <- reactable::reactable(
          data = dplyr::select(resSum, "isAtc2", "shortName", "rank", "avg", "nDatabases", "atc3Related", "atc4Related", "cohortDefinitionId2"),
          details = function(index) {
            cohortId <- resSum$cohortDefinitionId2[index]
            detailData <- resAll[resAll$shortName == resSum$shortName[index], c("databaseId", "cdmSourceAbbreviation", "numPersons", "cosineSimilarity", "cdmSpecificRankStr")]
            detailData <- detailData[order(detailData$cdmSourceAbbreviation),]

            selectionJs <- sprintf("
          function(rowInfo, column) {
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
                    cell = function() htmltools::tags$button("Explore Comparison")
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
              cell = function() htmltools::tags$button("Recommend Covariates for Exclusion")
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
      tagBox <- shiny::div(
        selectedCohortInfo(),
        shiny::h3(shiny::strong("Domain Similarity Scores")),
        shinycssloaders::withSpinner(reactable::reactableOutput(ns("cosineSimilarityTbl")))
      )

      shiny::showModal(
        shiny::modalDialog(
          title = "Covariate data",
          shiny::basicPage(
            shiny::tags$head(shiny::tags$style(".modal-dialog{ width:95%}")),
            tagBox,
            covariateUi(ns)
          ),
          size = "l",
          easyClose = FALSE,
          footer = shiny::tagList(
            shiny::actionButton(ns("closeModal"), "Close")
          )
        ))
    })

    shiny::observeEvent(input$show_exclusion, {
      shiny::showModal(
        shiny::modalDialog(
          title = "Recommend Covariates for Exclusion",
          exclusionCovariateUi(ns),
          ize = "l",
          easyClose = FALSE,
          footer = shiny::tagList(
            shiny::actionButton(ns("closeModal"), "Close")
          )
        )
      )
    })

    getCoOccurrenceData <- shiny::reactive({
      shiny::validate(shiny::need(input$selectedExposure, "must select exposure"),
                      shiny::need(input$show_exclusion$index, "must select comparator"))

      shiny::validate(shiny::need(input$prevInputHighMax > 0, "Threshold Inputs must be between 0 and 100"),
                      shiny::need(input$prevInputHighMax <= 100.0, "Threshold Inputs must be between 0 and 100"))

      shiny::validate(shiny::need(input$prevInputHighMin > 0, "Threshold Inputs must be between 0 and 100"),
                      shiny::need(input$prevInputHighMin <= 100.0, "Threshold Inputs must be between 0 and 100"))

      shiny::validate(shiny::need(input$prevInputLowMax > 0, "Threshold Inputs must be between 0 and 100"),
                      shiny::need(input$prevInputLowMax <= 100.0, "Threshold Inputs must be between 0 and 100"))

      shiny::validate(shiny::need(input$prevInputLowMin > 0, "Threshold Inputs must be between 0 and 100"),
                      shiny::need(input$prevInputLowMin <= 100.0, "Threshold Inputs must be between 0 and 100"))

      covData <- getCoOccurenceTableData(qns,
                                         databaseIds = input$selectedDatabases,
                                         prevInputHighMax = input$prevInputHighMax / 100,
                                         prevInputHighMin = input$prevInputHighMin / 100,
                                         prevInputLowMax = input$prevInputLowMax / 100,
                                         prevInputLowMin = input$prevInputLowMin / 100,
                                         cohortDefinitionId1 = selectedExposure(),
                                         cohortDefinitionId2 = input$show_exclusion$index)
      covData
    })

    allCohortDefinitions <- shiny::reactive(getCohortDefinitionsTable(qns, databaseId = selectedDatabases()))

    output$covTableCoOccurrence <- reactable::renderReactable({
      cohortDefinitions <- allCohortDefinitions()
      covData <- getCoOccurrenceData()

      targetName <- cohortDefinitions$shortName[cohortDefinitions$cohortDefinitionId == selectedExposure()][1]
      comparatorName <- cohortDefinitions$shortName[cohortDefinitions$cohortDefinitionId == input$show_exclusion$index][1]

      tableData <- covData |>
        dplyr::filter() |>
        dplyr::arrange(dplyr::desc(abs(.data$stdDiff))) |>
        dplyr::mutate(covariateShortName = gsub("concept co-occurrence:", "", .data$covariateShortName)) |>
        dplyr::mutate(conceptId = abs(.data$covariateId)) |>
        dplyr::select("conceptId", "cdmSourceAbbreviation", "covariateShortName", "mean1", "mean2", "stdDiff")

      reactable::reactable(
        data = tableData,
        columns = list(
          "cdmSourceAbbreviation" = reactable::colDef(name = "Data Source", align = "right", vAlign = "bottom"),
          "conceptId" = reactable::colDef(name = "Concept Id", align = "right", vAlign = "bottom"),
          "covariateShortName" = reactable::colDef(name = "Covariate", align = "right", vAlign = "bottom"),
          "mean1" = reactable::colDef(name = targetName, cell = function(value) { ifelse(value >= 0.01, scales::percent(value, accuracy = 0.1), "<1%") }, align = "center", vAlign = "bottom"),
          "mean2" = reactable::colDef(name = comparatorName, cell = function(value) { ifelse(value >= 0.01, scales::percent(value, accuracy = 0.1), "<1%") }, align = "center", vAlign = "bottom"),
          "stdDiff" = reactable::colDef(
            name = "Std. Diff.",
            cell = function(value, index) {
              if (tableData$mean1[index] >= 0.01 & tableData$mean2[index] >= 0.01) {
                sprintf(fmtSmd, value)
              } else {
                ifelse(tableData$mean1[index] < 0.01, paste0("(\u2265) ", sprintf(fmtSmd, value)), paste0("(\u2264) ", sprintf(fmtSmd, value)))
              }
            },
            align = "center",
            vAlign = "bottom")),
        bordered = TRUE,
        searchable = TRUE,
        showPageSizeOptions = TRUE,
        pageSizeOptions = c(5, 10, 20, 50, 100, 1000),
        striped = TRUE,
        highlight = TRUE,
        compact = TRUE,
        filterable = TRUE,
        theme = reactable::reactableTheme(
          borderColor = "#dfe2e5",
          stripedColor = "#f6f8fa",
          highlightColor = "#eab676",
          cellPadding = "8px 12px",
          searchInputStyle = list(width = "100%")),
        showSortIcon = TRUE)
    })

    shiny::observeEvent(input$closeModal, {
      shiny::removeModal()
    })

    shiny::outputOptions(output, "selectedComparator", suspendWhenHidden = FALSE)

    output$stepPlot <- plotly::renderPlotly({
      res <- getAllDbSimFiltered() |>
        dplyr::mutate(
          rank = dplyr::row_number(),
          tooltip = stringr::str_wrap(
            string = paste0(
              .data$shortName,
              " (",
              sprintf(fmtSim, .data$cosineSimilarity),
              ") #",
              prettyNum(.data$cdmSpecificRank, big.mark = ","),
              " of ",
              prettyNum(.data$comparatorsInCdm, big.mark = ",")),
            width = 20, indent = 1, exdent = 1))

      plotly::plot_ly(
        data = res,
        x = ~cdmSpecificRank,
        y = ~cosineSimilarity,
        color = ~cdmSourceAbbreviation,
        type = "scatter",
        mode = "lines",
        text = ~tooltip,
        hovertemplate = "%{text}") |>
        plotly::layout(
          hovermode = "x unified",
          xaxis = list(title = "Rank"),
          yaxis = list(title = "Cohort Similarity Score"),
          legend = list(orientation = 'h', y = -0.5))
    })

    getCovData <- shiny::reactive({
      shiny::validate(shiny::need(input$selectedExposure, "must select exposure"),
                      shiny::need(selectedComparator(), "must select comparator"))

      shiny::withProgress({
        covData <- getPairwiseCovariateData(qns,
                                            databaseId = selectedDatabase(),
                                            cohortDefinitionId1 = input$selectedExposure,
                                            cohortDefinitionId2 = selectedComparator())
      }, message = "Loading covariate data")
      covData
    })

    output$scatterPlot <- plotly::renderPlotly({
      shiny::validate(shiny::need(input$selectedExposure, 'must select exposure'),
                      shiny::need(selectedComparator(), 'must select comparator'))

      plot <- getCovData() |>
        dplyr::mutate(
          covariateShortName = gsub("Condition in <=30d prior:", "", .data$covariateShortName),
          covariateShortName = gsub("Condition in >30d prior:", "", .data$covariateShortName),
          covariateShortName = gsub("Drug with start >30d prior:", "", .data$covariateShortName),
          covariateShortName = stringr::str_to_sentence(gsub("<=30d prior|Visit:", "", .data$covariateShortName))) |>
        dplyr::mutate(
          type = NA,
          type = ifelse(.data$covariateType == "Demographics", "Demographics", .data$type),
          type = ifelse(.data$covariateType == "Presentation", "Presentation", .data$type),
          type = ifelse(.data$covariateType == "Medical history", "Medical History", .data$type),
          type = ifelse(.data$covariateType == "prior meds", "Prior Medications", .data$type),
          type = ifelse(.data$covariateType == "visit context", "Visit Context", .data$type),
          type = factor(.data$type, levels = c("Demographics", "Presentation", "Medical History", "Prior Medications", "Visit Context")),
          tooltip = paste0(
            "<b>",
            stringr::str_wrap(string = .data$covariateShortName, width = 20, indent = 1, exdent = 1),
            "</b>\n",
            "Target: ", ifelse(.data$mean1 < 0.01, "<1%", scales::percent(.data$mean1, accuracy = 0.1)), "\n",
            "Comparator: ", ifelse(.data$mean2 < 0.01, "<1%", scales::percent(.data$mean2, accuracy = 0.1)), "\n",
            "Std. Diff.: ", ifelse(.data$mean1 < 0.01 | .data$mean2 < 0.01,
                                   ifelse(.data$mean1 < 0.01, paste0("(\u2265) ", sprintf(fmtSmd, .data$stdDiff)), paste0("(\u2264) ", sprintf(fmtSmd, .data$stdDiff))),
                                   sprintf(fmtSmd, .data$stdDiff))
          )) |>
        plotly::plot_ly(
          type = 'scatter',
          mode = 'markers',
          x = ~mean1,
          y = ~mean2,
          color = ~type,
          text = ~tooltip,
          marker = list(opacity = 0.7),
          hovertemplate = "%{text}"
        ) |>
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

    output$smdPlot <- plotly::renderPlotly({
      shiny::validate(shiny::need(input$selectedExposure, 'must select exposure'),
                      shiny::need(selectedComparator(), 'must select comparator'))

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

      plot <- getCovData() |>
        dplyr::mutate(
          covariateShortName = gsub("Condition in <=30d prior:", "", .data$covariateShortName),
          covariateShortName = gsub("Condition in >30d prior:", "", .data$covariateShortName),
          covariateShortName = gsub("Drug with start >30d prior:", "", .data$covariateShortName),
          covariateShortName = stringr::str_to_sentence(gsub("<=30d prior|Visit:", "", .data$covariateShortName))) |>
        dplyr::mutate(
          type = NA,
          type = ifelse(.data$covariateType == "Demographics", "Demographics", .data$type),
          type = ifelse(.data$covariateType == "Presentation", "Presentation", .data$type),
          type = ifelse(.data$covariateType == "Medical history", "Medical History", .data$type),
          type = ifelse(.data$covariateType == "prior meds", "Prior Medications", .data$type),
          type = ifelse(.data$covariateType == "visit context", "Visit Context", .data$type),
          type = factor(.data$type, levels = (c("Demographics", "Presentation", "Medical History", "Prior Medications", "Visit Context"))),
          tooltip = paste0(
            "<b>",
            stringr::str_wrap(string = .data$covariateShortName, width = 20, indent = 1, exdent = 1),
            "</b>\n",
            "Target: ", ifelse(.data$mean1 < 0.01, "<1%", scales::percent(.data$mean1, accuracy = 0.1)), "\n",
            "Comparator: ", ifelse(.data$mean2 < 0.01, "<1%", scales::percent(.data$mean2, accuracy = 0.1)), "\n",
            "Std. Diff.: ", ifelse(.data$mean1 < 0.01 | .data$mean2 < 0.01,
                                   ifelse(.data$mean1 < 0.01, paste0("(\u2265) ", sprintf(fmtSmd, .data$stdDiff)), paste0("(\u2264) ", sprintf(fmtSmd, .data$stdDiff))),
                                   sprintf(fmtSmd, .data$stdDiff))
          )) |>
        plotly::plot_ly(
          hovertemplate = "%{text}"
        ) |>
        plotly::add_markers(
          x = ~stdDiff,
          y = ~jitter(as.numeric(type)),
          color = ~type,
          marker = list(opacity = 0.7),
          text = ~tooltip
        ) |>
        plotly::layout(
          xaxis = list(title = "Standardized Difference"),
          yaxis = list(title = "", showticklabels = FALSE),
          shapes = list(vline(-0.1), vline(0.1)),
          legend = list(orientation = 'h', y = -0.5))
    })

    inBalanceString <- function(covData) {
      inBalanceCount <- covData |>
        dplyr::filter(abs(.data$stdDiff) < 0.1) |>
        dplyr::count() |>
        dplyr::pull()

      percentBalanced <- round(inBalanceCount / nrow(covData) * 100, 1)
      paste(inBalanceCount, " of", nrow(covData), "covariates", paste0("(", percentBalanced, "%)"),
            "have absolute standardized difference less than 0.1")
    }

    output$covTableDemoBalance <- shiny::renderText({
      covData <- getCovData() |> dplyr::filter(.data$covariateType == "Demographics")
      inBalanceString(covData)
    })

    output$covTableDemo <- reactable::renderReactable({
      renderCovariateReactable(covariateType = "Demographics",
                               cohortDefinitionReactive = getCohortDefinitionsWithDbCounts,
                               covariateDataReactive = getCovData,
                               selectedExposure = selectedExposure,
                               selectedComparator = selectedComparator,
                               fmtSmd = fmtSmd,
                               covariateReplaceString = "",
                               stringToSentence = TRUE)
    })

    output$covTablePresBalance <- shiny::renderText({
      covData <- getCovData() |> dplyr::filter(.data$covariateType == "Presentation")
      inBalanceString(covData)
    })

    output$covTablePres <- reactable::renderReactable({
      renderCovariateReactable(covariateType = "Presentation",
                               cohortDefinitionReactive = getCohortDefinitionsWithDbCounts,
                               covariateDataReactive = getCovData,
                               selectedExposure = selectedExposure,
                               selectedComparator = selectedComparator,
                               fmtSmd = fmtSmd,
                               covariateReplaceString = "Condition in <=30d prior:")
    })

    output$covTableMhistBalance <- shiny::renderText({
      covData <- getCovData() |> dplyr::filter(.data$covariateType == "Medical history")
      inBalanceString(covData)
    })

    output$covTableMhist <- reactable::renderReactable({
      renderCovariateReactable(covariateType = "Medical history",
                               cohortDefinitionReactive = getCohortDefinitionsWithDbCounts,
                               covariateDataReactive = getCovData,
                               selectedExposure = selectedExposure,
                               selectedComparator = selectedComparator,
                               fmtSmd = fmtSmd,
                               covariateReplaceString = "Condition in >30d prior:")
    })

    output$covTablePmedsBalance <- shiny::renderText({
      covData <- getCovData() |> dplyr::filter(.data$covariateType == "prior meds")
      inBalanceString(covData)
    })

    output$covTablePmeds <- reactable::renderReactable({
      renderCovariateReactable(covariateType = "prior meds",
                               cohortDefinitionReactive = getCohortDefinitionsWithDbCounts,
                               covariateDataReactive = getCovData,
                               selectedExposure = selectedExposure,
                               selectedComparator = selectedComparator,
                               fmtSmd = fmtSmd,
                               covariateReplaceString = "Drug with start >30d prior:")
    })

    output$covTableVisitBalance <- shiny::renderText({
      covData <- getCovData() |> dplyr::filter(.data$covariateType == "visit context")
      inBalanceString(covData)
    })

    output$covTableVisit <- reactable::renderReactable({
      renderCovariateReactable(covariateType = "visit context",
                               cohortDefinitionReactive = getCohortDefinitionsWithDbCounts,
                               covariateDataReactive = getCovData,
                               selectedExposure = selectedExposure,
                               selectedComparator = selectedComparator,
                               fmtSmd = fmtSmd,
                               covariateReplaceString = "<=30d prior|Visit:")
    })

    output$dataSources <- reactable::renderReactable({
      getDbDataSourcesTable(qns)
    })

    output$covTableIndex <- reactable::renderReactable({
      renderCovariateReactable(covariateType = "Co-occurrence",
                               cohortDefinitionReactive = getCohortDefinitionsWithDbCounts,
                               covariateDataReactive = getCovData,
                               selectedExposure = selectedExposure,
                               selectedComparator = selectedComparator,
                               fmtSmd = fmtSmd,
                               covariateReplaceString = "concept co-occurrence:")
    })

    shiny::observeEvent(input$showRankings, {
      shiny::showModal(shiny::modalDialog(
        title = "Comparator Ranks",
        shiny::p(""),
        shinycssloaders::withSpinner(plotly::plotlyOutput(ns("stepPlot")))
      ))
    })
  })
}

#' Launch the Comparator Selection Shiny App
#' @description
#' Launches the full Shiny application for comparator selection, using the modular UI and server functions.
#'
#' @param connectionDetails DatabaseConnector connection details object.
#' @param resultsSchema Character. Results database schema.
#' @param tablePrefix Character. Optional table prefix for results tables. Default is "".
#' @param ... additional parameters to pass to shiny::shinyApp
#' @return A Shiny app object (invisibly; called for its side effect of launching the app).
#' @export
#' @examples
#' \dontrun{
#' createShinyApp(connectionDetails, "results_schema", tablePrefix = "myPrefix_")
#' }
createShinyApp <- function(connectionDetails, resultsSchema, tablePrefix = "", ...) {

  qns <- createResultsQueryNamespace(connectionDetails = connectionDetails, resultsSchema = resultsSchema, tablePrefix = tablePrefix, usePooledConnection = TRUE, ...)
  ui <- shiny::fluidPage(
    comparatorSelectionUi("main")
  )

  server <- function(input, output, session) {
    comparatorSelectionAppModuleServer("main", qns)
  }

  app <- shiny::shinyApp(ui = ui, server = server, onStart = function() {

    shiny::onStop(function() {
      qns$closeConnection()
    })
  }, ...)


  return(invisible(app))
}


#' Launch the Comparator Selection Shiny App
#'
#' Launches the full Shiny application for comparator selection, using the modular UI and server functions.
#'
#' @inheritParams createShinyApp
#' @return A Shiny app object (invisibly; called for its side effect of launching the app).
#' @export
#' @examples
#' \dontrun{
#' launchShinyApp(connectionDetails, "results_schema", tablePrefix = "myPrefix_")
#' }
launchShinyApp <- function(connectionDetails, resultsSchema, tablePrefix= "", ...) {
  app <- createShinyApp(connectionDetails, resultsSchema, tablePrefix, ...)
  shiny::runApp(app)
}
