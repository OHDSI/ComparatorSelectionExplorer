# ComparatorSelectionExplorer
This package is designed to aid the selection of comparators in active comparator, new user cohort designs by computing
a similarity metric across all possible pairs of drug comparators.

This package utilises code found within the [OHDSI HADES Library](https://ohdsi.github.com/HADES]) and is designed to
work on observational databases conforming to the OMOP CDM.

A [shiny](https://data.ohdsi.org/ComparatorSelectionExplorer) application is provided for the exploration of results.

## Setup
Follow the HADES setup guide to ensure your user space is correctly configured.
Following this, use remotes to install from github:

```r
remotes::install_github("OHDSI/ComparatorSelectionExplorer")
```

## Using this package
To run the full analysis:
```{r}
library(ComparatorSelectionExplorer)
connectionDetails <- createConnectionDetails(
 ...
)


executinSettings <- createExecutionSettings(connectionDetails = connectionDetails,
                                            databaseName = 'my_cdm',
                                            cdmDatabaseSchema = 'cdm',
                                            resultsDatabaseSchema = 'scratch') |> 
                        execute()
```
A zipfile will now be created at `executionSettings$exportZipFile`, this contains raw csv files including cosine
similarity scores and aggregated summary statistics for the full set of exposures in your cdm.

## Results exploration
You must first create a database schema in a database. In principle this can be any database engine, 
but for large results sets postgreql is recommended as we have implemented platform specific opitmizations.
If you only have a single CDM using an sqlite database should be fine.

For example:

```r
resultsConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite",
                                                                         server = "test.sqlite")
createResultsDataModel(resultsConnectionDetails, "main")

uploadResults(resultsConnectionDetails,
              "main",
              executionSettings$exportZipFile,
              tablePrefix = "")
```

## TODO
* Github repo and actions
* Shiny app changes
* Import results from a zip file
* Properly implement custom cohorts and allow them in shiny app