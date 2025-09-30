ComparatorSelectionExplorer 1.0.0
=================================

Breaking changes:

1. Use of cohort_templates from the cohort generator package

2. Removal of cohort generation within this package to simplify execution

3. Changes to results data model to reflect dependency on cohort generator

4. Added testing for most SqlRender supported database platforms

Changes:

1. Inclusion of parameters to allow adjustment of different weights in shiny app

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