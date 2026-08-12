ComparatorSelectionExplorer 1.0.0
=================================

Breaking changes:

1. Use of cohort_templates from the cohort generator package; cohort generation is now delegated to CohortGenerator::runCohortGeneration()

2. Removal of cohort generation within this package to simplify execution

3. Results data model updated with `cse_` table prefix and compatibility with CohortGenerator result tables (cg_cohort_definition, cg_cohort_count)

4. Added testing for most SqlRender-supported database platforms (bigquery, postgresql, redshift, spark, sql server, duckdb, sqlite)

5. `createExecutionSettings()` now requires `cohortTable` parameter; `connectionDetails` is optional if `connection` is provided; deprecated `targetCohortIds` in favor of `cohortTags`

6. `execute()` no longer creates cohorts internally — callers must run CohortGenerator first

Changes:

1. New cohort tagging system (`cohortTags` parameter) enabling computation of cosine similarity only between cohorts sharing the same tag

2. Modular Shiny application (`R/Shiny-Server.R`, `R/Shiny-Ui.R`) with cross-database ranking, weighted similarity scores, and covariate exclusion exploration

3. Inclusion of parameters to allow adjustment of different domain weights in Shiny app

4. New results query namespace (`createResultsQueryNamespace()`) for interacting with the results data model

5. New utility functions: `createShinyApp()`, `launchShinyApp()`

6. New SQL migration scripts for v1.0.0 schema changes

7. Comprehensive test suite: platform query validation via sqlglot, tag processing, results queries, Shiny server and UI tests

ComparatorSelectionExplorer 0.3.0
=================================

Changes:

1. shiny app now does a cross database ranking of results

2. Shiny app layouts and buttons changed

3. Added index date covariates (not used in cosine similarity scoring) as possible covariates to exclude from propensity
score matching

4. Added support for computing subsets of exposure using cohort generator subset functionality. This allows computation
for specific sub populations that may be beneficial when selecting appropriate comparators (e.g. only patients with
a specific indication exposed to a drug)

5. Objects that use executionSettings now modify input (and return it) which should make writing targets workflows
simpler

ComparatorSelectionExplorer 0.2.0
=================================

Changes:

* Added functionality to support viewing prevalence of covariates that occur on the day of index to enable the 
exploration of those to exclude in propensity score matching