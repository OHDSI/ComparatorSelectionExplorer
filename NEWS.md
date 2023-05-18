ComparatorSelectionExplorer 0.2.0
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