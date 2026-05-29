# ComparatorSelectionExplorer API Client MCP Skill

## Purpose

Use this skill when the user asks to query a running Comparator Selection Explorer API
from R code (instead of using raw `httr` requests).

The package helper `createApiClient()` provides an opinionated client with methods:

- `health()`
- `databases()`
- `search(q, tag)`
- `rankings(targetCohortId, database_ids, min_databases, comparator_type, weight_*)`
- `compare(cohortId1, cohortId2, database_id)`

## When To Use

- User asks for examples calling the package API from R scripts.
- User wants cohort search, rankings, or pairwise comparison against a running API server.
- User wants weight tuning examples for comparator ranking.

## When Not To Use

- User needs server setup or route implementation details (use Plumber API setup docs).
- User asks for direct SQL/database queries (use query helpers instead).

## Required Verification Before Writing Calls

Before generating or editing code, verify function signatures in terminal:

1. `args(ComparatorSelectionExplorer::createApiClient)`
2. `names(ComparatorSelectionExplorer::createApiClient() )`

If the API host/port is custom, confirm expected base URL with the user.

## Usage Pattern

```r
library(ComparatorSelectionExplorer)

client <- createApiClient(host = "127.0.0.1", port = 8080)

# Health check
client$health()

# Available source databases
client$databases()

# Cohort search
client$search(q = "ibuprofen", tag = "ATC")

# Comparator rankings for one target
client$rankings(
  targetCohortId = 101,
  database_ids = c("CCAE", "MDCR"),
  min_databases = 2,
  comparator_type = "ATC",
  weight_demo = 20,
  weight_pres = 20,
  weight_hist = 20,
  weight_meds = 20,
  weight_visit = 20
)

# Domain-level similarity breakdown for a pair
client$compare(cohortId1 = 101, cohortId2 = 202, database_id = "CCAE")
```

## Error Handling Guidance

- API calls raise errors when HTTP status is not successful.
- For troubleshooting, verify API is running and call `health()` first.
- If ranking queries fail, confirm target cohort exists and database IDs are valid.

## Related Package Functions

- `startComparatorApi()` to start the local API server.
- `createShinyApp()` for UI exploration of the same results data.