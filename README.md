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
 
)

execute(connectionDetails = connectionDetails,)

```

## Results exploration


## TODO
* Github repo and actions
* Shiny app changes
* Export results from tables
* Results data model creation