# packages
library(shiny)
library(ggplot2)
library(scales)
library(tidyr)
library(dplyr, warn.conflicts = FALSE)

# Override default DatabaseConnector query function
renderTranslateQuerySql <-
  function(connection,
           sql,
           dbms,
           ...,
           snakeCaseToCamelCase = FALSE) {
    if (is(connection, "Pool")) {
      sql <- SqlRender::render(sql, ...)
      sql <- SqlRender::translate(sql, targetDialect = dbms)

      tryCatch(
      {
        data <- DatabaseConnector::dbGetQuery(connection, sql)
      },
        error = function(err) {
          writeLines(sql)
          if (dbms %in% c("postgresql", "redshift")) {
            DatabaseConnector::dbExecute(connection, "ABORT;")
          }
          stop(err)
        }
      )
      if (snakeCaseToCamelCase) {
        colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
      }
      return(data)
    } else {
      return(
        DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = sql,
          ...,
          snakeCaseToCamelCase = snakeCaseToCamelCase
        )
      )
    }
  }

conn <-
  pool::dbPool(
    drv = DatabaseConnector::DatabaseConnectorDriver(),
    dbms = connectionDetails$dbms,
    server = connectionDetails$server(),
    port = connectionDetails$port(),
    user = connectionDetails$user(),
    password = connectionDetails$password(),
    connectionString = connectionDetails$connectionString()
  )

# Handle closing connection cleanly
shiny::onStop(function() {
  if (DBI::dbIsValid(conn)) {
    writeLines("Closing database pool")
    pool::poolClose(conn)
  }
})

# Define server logic required to draw a histogram
shinyServer(function(input, output, session) {
  databaseSelection <- shiny::reactive({
    input$selectedDatabase
  })

  getCohortDefinitions <- shiny::reactive({
    renderTranslateQuerySql(
      connection = conn,
      sql = "select distinct
               t.cohort_definition_id,
               short_name,
               atc_flag as is_atc
             from @schema.@table_prefix@table t
             where t.cohort_definition_id is not null
             and   atc_flag in (0, 1)
             order by short_name",
      dbms = connectionDetails$dbms,
      schema = resultsSchema,
      table = "cohort_definition",
      table_prefix = tablePrefix,
      snakeCaseToCamelCase = TRUE
    )
  })


  # initial query to get list of cohort definitions
  getCohortDefinitionsWithCounts <- shiny::reactive({
    dbSel <- databaseSelection()
    if (is.null(dbSel) || dbSel == "")
      return(data.frame())

    renderTranslateQuerySql(
      connection = conn,
      sql = "select distinct
               t.cohort_definition_id,
               short_name,
               atc_flag as is_atc,
               c.num_persons
             from @schema.@table_prefix@table t
             inner join @schema.@table_prefixcohort_count c ON c.cohort_definition_id = t.cohort_definition_id
             where t.cohort_definition_id is not null
             and   atc_flag in (0, 1)
             and c.database_id = @database_id
             order by short_name",
      dbms = connectionDetails$dbms,
      schema = resultsSchema,
      table = "cohort_definition",
      database_id = dbSel,
      table_prefix = tablePrefix,
      snakeCaseToCamelCase = TRUE
    )
  })

  getDatabaseSources <- shiny::reactive({
    renderTranslateQuerySql(
      connection = conn,
      sql = "select distinct *
             from @schema.@table_prefix@table t",
      dbms = connectionDetails$dbms,
      schema = resultsSchema,
      table = "cdm_source_info",
      table_prefix = tablePrefix,
      snakeCaseToCamelCase = TRUE
    )
  })

  observe({
    shiny::withProgress({
      dbSources <- getDatabaseSources()
      dbChoices <- dbSources$databaseId
      names(dbChoices) <- dbSources$cdmSourceAbbreviation

      updateSelectizeInput(
        session,
        "selectedDatabase",
        choices = dbChoices,
        selected = dbChoices[1],
        server = TRUE)
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


  #### ---- function to get cosine similarity data (for table and plot) ---- ####
  getSimilarity <- shiny::reactive({
    # identify target cohort
    targetCohortId <- input$selectedExposure
    validate(need(input$selectedExposure, "must select exposure"))

    shiny::withProgress({
      # identify selected comparator types
      if (length(input$selectedComparatorTypes) == 2L) { atcSelection <- c(0, 1) }
      else if (input$selectedComparatorTypes == "RxNorm Ingredients") { atcSelection <- c(0) }
      else if (input$selectedComparatorTypes == "ATC Classes") { atcSelection <- c(1) }
      # send query to get results data
      resultsData <- renderTranslateQuerySql(
        connection = conn,
        sql = "
            select
               t.cohort_definition_id_2,
               cd2.atc_flag as is_atc_2,
               cd2.short_name,
               cosine_similarity,
               atc.atc_4_related,
               atc.atc_3_related,
               ec.num_persons,
               t.covariate_type
             from @schema.@table_prefix@table t
             inner join @schema.@table_prefixcohort_count ec ON ec.cohort_definition_id = t.cohort_definition_id_2
             inner join @schema.@table_prefixcohort_definition cd2 ON cd2.cohort_definition_id = t.cohort_definition_id_2

             inner join @schema.@table_prefixatc_level atc on t.cohort_definition_id_1 = atc.cohort_definition_id_1 and t.cohort_definition_id_2 = atc.cohort_definition_id_2
             where t.cohort_definition_id_1 = @targetCohortId
             and cd2.atc_flag in (@atc)
             AND ec.num_persons >= 1000
             and t.database_id = @database_id
           ",
        dbms = connectionDetails$dbms,
        table_prefix = tablePrefix,
        schema = resultsSchema,
        table = "cosine_similarity_score",
        snakeCaseToCamelCase = TRUE,
        targetCohortId = targetCohortId,
        database_id = databaseSelection(),
        atc = atcSelection)
    }, message = "Loading similarity scores", value = 0.5)

    resultsData %>%
        tidyr::pivot_wider(names_from = covariateType,
                           values_from = cosineSimilarity) %>%
        dplyr::rename(cosineSimAll = average,
                      cosineSimDemo = Demographics,
                      cosineSimPres = Presentation,
                      cosineSimMhist = "Medical history",
                      cosineSimPmeds = "prior meds",
                      cosineSimVisit = "visit context")

  })

  #### ---- cosine similarity reactable ---- ####
  output$cosineSimilarityTbl <- reactable::renderReactable({
    res <- getSimilarity() %>% dplyr::select(-cohortDefinitionId2)
    shiny::withProgress({
      rt <- reactable::reactable(
        data = res,
        columns = list(
          "isAtc2" = reactable::colDef(name = "Type", cell = function(value) { ifelse(value == 1, "ATC Class", "RxNorm Ingredient") }, align = "right", vAlign = "center", headerVAlign = "bottom", minWidth = 125),
          "cosineSimAll" = reactable::colDef(name = "Avg.", cell = function(value) { sprintf("%.3f", value) }, align = "center", vAlign = "center", headerVAlign = "bottom", minWidth = 125),
          "cosineSimDemo" = reactable::colDef(name = "Demographics", cell = function(value) { sprintf("%.3f", value) }, align = "center", vAlign = "center", headerVAlign = "bottom", minWidth = 125),
          "cosineSimPres" = reactable::colDef(name = "Presentation", cell = function(value) { sprintf("%.3f", value) }, align = "center", vAlign = "center", headerVAlign = "bottom", minWidth = 125),
          "cosineSimMhist" = reactable::colDef(name = "Medical History", cell = function(value) { sprintf("%.3f", value) }, align = "center", vAlign = "center", headerVAlign = "bottom", minWidth = 125),
          "cosineSimPmeds" = reactable::colDef(name = "Prior Medications", cell = function(value) { sprintf("%.3f", value) }, align = "center", vAlign = "center", headerVAlign = "bottom", minWidth = 125),
          "cosineSimVisit" = reactable::colDef(name = "Visit Context", cell = function(value) { sprintf("%.3f", value) }, align = "center", vAlign = "center", headerVAlign = "bottom", minWidth = 125),
          "shortName" = reactable::colDef(name = "Name", cell = function(value) { ifelse(substr(value, 1, 6) == "RxNorm", gsub("RxNorm - ", "", value), gsub("ATC - ", "", value)) }, align = "left", vAlign = "center", headerVAlign = "bottom", minWidth = 125),
          "numPersons" = reactable::colDef(name = "Sample size", cell = function(value) format(round(value), big.mark=","), align = "center", vAlign = "center", headerVAlign = "bottom", filterable = TRUE),
          "atc3Related" = reactable::colDef(name = "At Level 3", cell = function(value) ifelse(is.na(value) | value == 0, "No", "Yes"), align = "center", vAlign = "center", headerVAlign = "bottom", filterable = TRUE),
          "atc4Related" = reactable::colDef(name = "At Level 4", cell = function(value) ifelse(is.na(value) | value == 0, "No", "Yes"), align = "center", vAlign = "center", headerVAlign = "bottom", filterable = TRUE)),
        searchable = TRUE,
        columnGroups = list(
          reactable::colGroup(
            "Comparator",
            c("shortName", "isAtc2")),
          reactable::colGroup(
            "Covariate Domain",
            c("cosineSimAll", "cosineSimDemo", "cosineSimPres", "cosineSimMhist", "cosineSimPmeds", "cosineSimVisit")),
          reactable::colGroup(
            "ATC Relationship to Target",
            c("atc3Related", "atc4Related"))),
        fullWidth = TRUE,
        bordered = TRUE,
        showPageSizeOptions = TRUE,
        pageSizeOptions = c(5, 10, 20, 50, 100, 1000),
        striped = TRUE,
        highlight = TRUE,
        compact = TRUE,
        selection = "single",
        theme = reactable::reactableTheme(
          borderColor = "#dfe2e5",
          stripedColor = "#f6f8fa",
          highlightColor = "#eab676",
          cellPadding = "8px 12px",
          searchInputStyle = list(width = "100%")),
        showSortIcon = TRUE)
    }, message = "Rendering results", value = 0.7)

    rt
  })

  selectedComparator <- shiny::reactive({
    selection <- reactable::getReactableState("cosineSimilarityTbl", name = "selected")
    row <- getSimilarity()[selection,]
    row$cohortDefinitionId2
  })

  output$selectedComparator <- shiny::reactive({
    selection <- reactable::getReactableState("cosineSimilarityTbl", name = "selected")
    return(!is.null(selection))
  })

  shiny::outputOptions(output,
                       "selectedComparator",
                       suspendWhenHidden = FALSE)

  #### ---- step-function plot of cosine similarity by rank ---- ####
  output$stepPlot <- renderPlot({
    getSimilarity() %>%
      pivot_longer(
        c(cosineSimAll, cosineSimDemo, cosineSimPres, cosineSimMhist, cosineSimPmeds, cosineSimVisit),
        values_to = "cosineSimilarity",
        names_to = "var") %>%
      mutate(simType = NA,
             simType = ifelse(var == "cosineSimAll", "Avg.", simType),
             simType = ifelse(var == "cosineSimDemo", "Demographics", simType),
             simType = ifelse(var == "cosineSimPres", "Presentation", simType),
             simType = ifelse(var == "cosineSimMhist", "Medical History", simType),
             simType = ifelse(var == "cosineSimPmeds", "Prior Medications", simType),
             simType = ifelse(var == "cosineSimVisit", "Visit Context", simType),
             simType = factor(simType, levels = c("Avg.", "Demographics", "Presentation", "Medical History", "Prior Medications", "Visit Context"))) %>%
      group_by(simType) %>%
      mutate(simRank = row_number(desc(cosineSimilarity))) %>%
      ungroup() %>%
      ggplot(aes(x = simRank, y = cosineSimilarity, color = simType)) +
      geom_step() +
      scale_x_continuous(labels = comma) +
      scale_color_manual(
        values = c("Avg." = "#4E79A7",
                   "Demographics" = "#F28E2B",
                   "Presentation" = "#E15759",
                   "Medical History" = "#76B7B2",
                   "Prior Medications" = "#59A14F",
                   "Visit Context" = "#EDC948")) +
      labs(y = "Cosine Similarity",
           x = "Comparator Rank",
           color = "Covariate Domain") +
      theme_minimal(base_size = 9) +
      theme(legend.position = "bottom") +
      guides(color = guide_legend(ncol = 2))
  })

  ##### ---- function to get covariate data for a given comparison ---- ####
  getCovData <- shiny::reactive({
    validate(need(input$selectedExposure, "must select exposure"),
             need(selectedComparator(), "must select comparator"))

    shiny::withProgress({
      covData <- renderTranslateQuerySql(
        connection = conn,
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
              	  select t.*, covd.covariate_name, covd.covariate_type from @schema.@table_prefix@table t
              	  inner join @schema.@table_prefixcovariate_definition covd on covd.covariate_id = t.covariate_id
              	  where t.cohort_definition_id = @cohortDefinitionId1
              	  and t.database_id = @database_id
            	  ) as c1
              	full join (
              	  select t.*, covd.covariate_name, covd.covariate_type from @schema.@table_prefix@table t
              	  inner join @schema.@table_prefixcovariate_definition covd on covd.covariate_id = t.covariate_id
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
              join @schema.@table_prefixcohort_count as c1
              	on m.cohort_definition_id_1 = c1.cohort_definition_id and c1.database_id = @database_id
              join @schema.@table_prefixcohort_count as c2
              	on m.cohort_definition_id_2 = c2.cohort_definition_id and c2.database_id = @database_id
              ;",
        dbms = connectionDetails$dbms,
        snakeCaseToCamelCase = TRUE,
        schema = resultsSchema,
        table = "covariate_mean",
        table_prefix = tablePrefix,
        database_id = databaseSelection(),
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
      mutate(type = NA,
             type = ifelse(covariateType == "Demographic", "Demographics", type),
             type = ifelse(covariateType == "Presentation", "Presentation", type),
             type = ifelse(covariateType == "Medical history", "Medical History", type),
             type = ifelse(covariateType == "prior meds", "Prior Medications", type),
             type = ifelse(covariateType == "visit context", "Visit Context", type),
             type = factor(type, levels = c("Demographics", "Presentation", "Medical History", "Prior Medications", "Visit Context"))) %>%
      ggplot(aes(x = mean1, y = mean2, color = type)) +
      geom_abline(linetype = 2) +
      geom_point(alpha = 0.7) +
      scale_color_manual(
        values = c("Demographics" = "#F28E2B",
                   "Presentation" = "#E15759",
                   "Medical History" = "#76B7B2",
                   "Prior Medications" = "#59A14F",
                   "Visit Context" = "#EDC948")) +
      guides(color = guide_legend(override.aes = list(alpha = 1))) +
      theme_minimal(base_size = 9) +
      theme(legend.position = "bottom") +
      guides(color = guide_legend(ncol = 2)) +
      labs(x = "Prevalence in Target Cohort",
           y = "Prevalence in Comparator Cohort",
           color = "Covariate Domain")


    plotly::ggplotly(plot)
  })

  #### ---- plot of std. diffs. ---- ####
  output$smdPlot <- plotly::renderPlotly({
    shiny::validate(need(input$selectedExposure, 'must select exposure'),
                    need(selectedComparator(), 'must select comparator'))
    plot <- getCovData() %>%
      mutate(type = NA,
             type = ifelse(covariateType == "Demographics", "Demographics", type),
             type = ifelse(covariateType == "Presentation", "Presentation", type),
             type = ifelse(covariateType == "Medical history", "Medical History", type),
             type = ifelse(covariateType == "prior meds", "Prior Medications", type),
             type = ifelse(covariateType == "visit context", "Visit Context", type),
             type = factor(type, levels = c("Demographics", "Presentation", "Medical History", "Prior Medications", "Visit Context"))) %>%
      ggplot(aes(x = stdDiff, y = type, color = type)) +
      geom_vline(aes(xintercept = 0)) +
      geom_vline(aes(xintercept = 0.1), linetype = 2) +
      geom_vline(aes(xintercept = -0.1), linetype = 2) +
      geom_point(position = position_jitter(), alpha = 0.7) +
      scale_color_manual(
        values = c("Demographics" = "#F28E2B",
                   "Presentation" = "#E15759",
                   "Medical History" = "#76B7B2",
                   "Prior Medications" = "#59A14F",
                   "Visit Context" = "#EDC948")) +
      theme_minimal(base_size = 9) +
      theme(legend.position = "none",
            panel.grid.major.y = element_blank()) +
      guides(color = guide_legend(ncol = 2)) +
      labs(x = "Standardized Difference",
           y = "",
           color = "Covariate Domain")


    plotly::ggplotly(plot)
  })

  inBalanceString <- function(covData) {
    inBalanceCount <- covData %>%
      filter(abs(stdDiff) < 0.1) %>%
      count() %>%
      pull()

    percentBalanced <- round(inBalanceCount/nrow(covData) * 100, 1)
    paste( inBalanceCount, " of", nrow(covData), "covariates", paste0("(", percentBalanced, "%)"),
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
    colNameTarget <- paste0(
      cohortDefinitions$cohortShortName[cohortDefinitions$cohortDefinitionId == input$selectedExposure],
      " (n = ",
      prettyNum(first(covData$n1), big.mark = ","),
      ")")

    colNameComparator <- paste0(
      cohortDefinitions$cohortShortName[cohortDefinitions$cohortDefinitionId == selectedComparator()],
      " (n = ",
      prettyNum(first(covData$n2), big.mark = ","),
      ")")

    # subset data and select relevant columns
    tableData <- covData %>%
      filter(covariateType == "Demographics") %>%
      arrange(desc(covariateShortName)) %>%
      select(covariateShortName, mean1, mean2, stdDiff)

    # table code
    reactable::reactable(
      data = tableData,
      columns = list(
        "covariateShortName" = reactable::colDef(name = "Covariate", align = "right", vAlign = "bottom"),
        "mean1" = reactable::colDef(name = colNameTarget, cell = function(value) { percent(value, accuracy = 0.1) }, align = "center", vAlign = "bottom"),
        "mean2" = reactable::colDef(name = colNameComparator, cell = function(value) { percent(value, accuracy = 0.1) }, align = "center", vAlign = "bottom"),
        "stdDiff" = reactable::colDef(name = "Std. Diff.", cell = function(value) { sprintf("%.2f", value) }, align = "center", vAlign = "bottom")),
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
    colNameTarget <- paste0(
      cohortDefinitions$cohortShortName[cohortDefinitions$cohortDefinitionId == input$selectedExposure],
      " (n = ",
      prettyNum(first(covData$n1), big.mark = ","),
      ")")

    colNameComparator <- paste0(
      cohortDefinitions$cohortShortName[cohortDefinitions$cohortDefinitionId == selectedComparator()],
      " (n = ",
      prettyNum(first(covData$n2), big.mark = ","),
      ")")

    # subset data and select relevant columns
    tableData <- covData %>%
      filter(covariateType == "Presentation") %>%
      arrange(desc(abs(stdDiff))) %>%
      select(covariateShortName, mean1, mean2, stdDiff)

    # table code
    reactable::reactable(
      data = tableData,
      columns = list(
        "covariateShortName" = reactable::colDef(name = "Covariate", align = "right", vAlign = "bottom"),
        "mean1" = reactable::colDef(name = colNameTarget, cell = function(value) { percent(value, accuracy = 0.1) }, align = "center", vAlign = "bottom"),
        "mean2" = reactable::colDef(name = colNameComparator, cell = function(value) { percent(value, accuracy = 0.1) }, align = "center", vAlign = "bottom"),
        "stdDiff" = reactable::colDef(name = "Std. Diff.", cell = function(value) { sprintf("%.2f", value) }, align = "center", vAlign = "bottom")),
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
    colNameTarget <- paste0(
      cohortDefinitions$cohortShortName[cohortDefinitions$cohortDefinitionId == input$selectedExposure],
      " (n = ",
      prettyNum(first(covData$n1), big.mark = ","),
      ")")

    colNameComparator <- paste0(
      cohortDefinitions$cohortShortName[cohortDefinitions$cohortDefinitionId == selectedComparator()],
      " (n = ",
      prettyNum(first(covData$n2), big.mark = ","),
      ")")

    # subset data and select relevant columns
    tableData <- covData %>%
      filter(covariateType == "Medical history") %>%
      arrange(desc(abs(stdDiff))) %>%
      select(covariateShortName, mean1, mean2, stdDiff)

    # table code
    reactable::reactable(
      data = tableData,
      columns = list(
        "covariateShortName" = reactable::colDef(name = "Covariate", align = "right", vAlign = "bottom"),
        "mean1" = reactable::colDef(name = colNameTarget, cell = function(value) { percent(value, accuracy = 0.1) }, align = "center", vAlign = "bottom"),
        "mean2" = reactable::colDef(name = colNameComparator, cell = function(value) { percent(value, accuracy = 0.1) }, align = "center", vAlign = "bottom"),
        "stdDiff" = reactable::colDef(name = "Std. Diff.", cell = function(value) { sprintf("%.2f", value) }, align = "center", vAlign = "bottom")),
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
    colNameTarget <- paste0(
      cohortDefinitions$cohortShortName[cohortDefinitions$cohortDefinitionId == input$selectedExposure],
      " (n = ",
      prettyNum(first(covData$n1), big.mark = ","),
      ")")

    colNameComparator <- paste0(
      cohortDefinitions$cohortShortName[cohortDefinitions$cohortDefinitionId == selectedComparator()],
      " (n = ",
      prettyNum(first(covData$n2), big.mark = ","),
      ")")

    # subset data and select relevant columns
    tableData <- covData %>%
      filter(covariateType == "prior meds") %>%
      arrange(desc(abs(stdDiff))) %>%
      select(covariateShortName, mean1, mean2, stdDiff)

    # table code
    reactable::reactable(
      data = tableData,
      columns = list(
        "covariateShortName" = reactable::colDef(name = "Covariate", align = "right", vAlign = "bottom"),
        "mean1" = reactable::colDef(name = colNameTarget, cell = function(value) { percent(value, accuracy = 0.1) }, align = "center", vAlign = "bottom"),
        "mean2" = reactable::colDef(name = colNameComparator, cell = function(value) { percent(value, accuracy = 0.1) }, align = "center", vAlign = "bottom"),
        "stdDiff" = reactable::colDef(name = "Std. Diff.", cell = function(value) { sprintf("%.2f", value) }, align = "center", vAlign = "bottom")),
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

  #### ---- "table 1": visit context (default to unsorted) ---- ####
  output$covTableVisit <- reactable::renderReactable({
    cohortDefinitions <- getCohortDefinitionsWithCounts()
    # get data
    covData <- getCovData()

    shiny::withProgress({
      # create column names with cohort sample sizes
      colNameTarget <- paste0(
        cohortDefinitions$cohortShortName[cohortDefinitions$cohortDefinitionId == input$selectedExposure],
        " (n = ",
        prettyNum(first(covData$n1), big.mark = ","),
        ")")

      colNameComparator <- paste0(
        cohortDefinitions$cohortShortName[cohortDefinitions$cohortDefinitionId == selectedComparator()],
        " (n = ",
        prettyNum(first(covData$n2), big.mark = ","),
        ")")

      # subset data and select relevant columns
      tableData <- covData %>%
        filter(covariateType == "visit context") %>%
        arrange(covariateShortName) %>%
        select(covariateShortName, mean1, mean2, stdDiff)

      # table code
      rt <- reactable::reactable(
        data = tableData,
        columns = list(
          "covariateShortName" = reactable::colDef(name = "Covariate", align = "right", vAlign = "bottom"),
          "mean1" = reactable::colDef(name = colNameTarget, cell = function(value) { percent(value, accuracy = 0.1) }, align = "center", vAlign = "bottom"),
          "mean2" = reactable::colDef(name = colNameComparator, cell = function(value) { percent(value, accuracy = 0.1) }, align = "center", vAlign = "bottom"),
          "stdDiff" = reactable::colDef(name = "Std. Diff.", cell = function(value) { sprintf("%.2f", value) }, align = "center", vAlign = "bottom")),
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
})
