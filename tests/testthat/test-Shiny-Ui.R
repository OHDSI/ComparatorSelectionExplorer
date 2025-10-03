test_that("comparatorSelectionUi renders expected elements", {
  ui <- comparatorSelectionUi("test")
  html <- as.character(ui)

  # Check for dashboard header
  expect_true(grepl("Comparator Selection Explorer", html))

  # Check for tab names
  expect_true(grepl("Recommend Comparators", html))
  expect_true(grepl("About", html))

  # Check for selectize/selectInput ids
  expect_true(grepl('id="test-selectedExposure"', html))
  expect_true(grepl('id="test-selectedComparatorTypes"', html))
  expect_true(grepl('id="test-selectedDatabases"', html))

  # Check for weights slider inputs
  expect_true(grepl('id="test-userWeightDemo"', html))
  expect_true(grepl('id="test-userWeightPres"', html))
  expect_true(grepl('id="test-userWeightHist"', html))
  expect_true(grepl('id="test-userWeightMeds"', html))
  expect_true(grepl('id="test-userWeightVisit"', html))

  # Check for action buttons
  expect_true(grepl('id="test-getResults"', html))
  expect_true(grepl('id="test-showRankings"', html))
})

test_that("exclusionCovariateUi renders expected elements", {
  ns <- shiny::NS("test")
  ui <- exclusionCovariateUi(ns)
  html <- as.character(ui)

  # Check for numeric inputs
  expect_true(grepl('id="test-prevInputHighMax"', html))
  expect_true(grepl('id="test-prevInputHighMin"', html))
  expect_true(grepl('id="test-prevInputLowMax"', html))
  expect_true(grepl('id="test-prevInputLowMin"', html))

  # Check for download button
  expect_true(grepl("Download", html))
  expect_true(grepl("covTableCoOccurrence", html))
})

test_that("covariateUi renders expected elements", {
  ns <- shiny::NS("test")
  ui <- covariateUi(ns)

  # Should be a tagList
  expect_true(inherits(ui, "shiny.tag.list"))

  # Convert to HTML for text checks
  html <- as.character(ui)

  # Check header text
  expect_true(grepl("Visualizations", html))
  expect_true(grepl("Covariate Tables", html))

  # Check that plotly outputs are present and namespaced
  expect_true(grepl('id="test-scatterPlot"', html))
  expect_true(grepl('id="test-smdPlot"', html))

  # Check for all tab names
  expect_true(grepl("Demographics", html))
  expect_true(grepl("Presentation", html))
  expect_true(grepl("Medical history", html))
  expect_true(grepl("Prior medications", html))
  expect_true(grepl("Visit context", html))
  expect_true(grepl("Index date", html))

  # Check for reactable outputs (tables)
  expect_true(grepl('id="test-covTableDemo"', html))
  expect_true(grepl('id="test-covTablePres"', html))
  expect_true(grepl('id="test-covTableMhist"', html))
  expect_true(grepl('id="test-covTablePmeds"', html))
  expect_true(grepl('id="test-covTableVisit"', html))
  expect_true(grepl('id="test-covTableIndex"', html))
})
