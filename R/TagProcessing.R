# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of ComparatorSelectionExplorer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#' Validate cohort tags structure
#' @description
#' Validates that cohortTags is either NULL or a properly formatted named list
#' @param cohortTags Named list where names are tag names and values are integer vectors of cohort IDs
#' @returns TRUE if valid, throws error otherwise
#' @keywords internal
validateCohortTags <- function(cohortTags) {
  if (is.null(cohortTags)) {
    return(TRUE)
  }
  
  # Check if it's a list
  if (!is.list(cohortTags)) {
    stop("cohortTags must be a named list")
  }
  
  # Check if it has names
  if (is.null(names(cohortTags)) || any(names(cohortTags) == "")) {
    stop("cohortTags must be a named list with non-empty tag names")
  }
  
  # Check if all values are numeric/integer vectors
  for (tagName in names(cohortTags)) {
    if (!is.numeric(cohortTags[[tagName]])) {
      stop(paste0("All cohort IDs in cohortTags must be numeric. Tag '", tagName, "' contains non-numeric values"))
    }
  }
  
  return(TRUE)
}


#' Flatten cohort tags to unique cohort IDs
#' @description
#' Extracts all unique cohort IDs from a nested list of tags
#' @param cohortTags Named list where names are tag names and values are integer vectors of cohort IDs
#' @returns Integer vector of unique cohort IDs
#' @keywords internal
flattenCohortTags <- function(cohortTags) {
  if (is.null(cohortTags)) {
    return(NULL)
  }
  
  validateCohortTags(cohortTags)
  
  # Extract all cohort IDs and get unique values
  allCohortIds <- unique(unlist(cohortTags, use.names = FALSE))
  return(as.integer(allCohortIds))
}


#' Convert cohort tags to data frame
#' @description
#' Converts nested list of tags to a data frame with columns: cohortDefinitionId, tag
#' @param cohortTags Named list where names are tag names and values are integer vectors of cohort IDs
#' @returns Data frame with columns cohortDefinitionId (integer) and tag (character)
#' @keywords internal
cohortTagsToDataFrame <- function(cohortTags) {
  if (is.null(cohortTags)) {
    return(data.frame(cohortDefinitionId = integer(0), tag = character(0)))
  }
  
  validateCohortTags(cohortTags)
  
  # Convert to long format data frame
  tagsList <- list()
  for (tagName in names(cohortTags)) {
    cohortIds <- cohortTags[[tagName]]
    tagsList[[tagName]] <- data.frame(
      cohortDefinitionId = as.numeric(cohortIds),
      tag = tagName,
      stringsAsFactors = FALSE
    )
  }
  
  tagsDataFrame <- do.call(rbind, tagsList)
  rownames(tagsDataFrame) <- NULL
  
  return(tagsDataFrame)
}


#' Create temporary table for cohort tags in database
#' @description
#' Creates a temporary table #cse_cohort_tags with cohort IDs and their tags
#' @param cohortTags Named list where names are tag names and values are integer vectors of cohort IDs
#' @param connection DatabaseConnector connection object
#' @returns TRUE if successful, throws error otherwise
#' @keywords internal
cohortTagsToTempTable <- function(cohortTags, connection) {
  if (is.null(cohortTags)) {
    return(FALSE)
  }
  
  validateCohortTags(cohortTags)
  
  # Convert to data frame
  tagsDataFrame <- cohortTagsToDataFrame(cohortTags)
  
  if (nrow(tagsDataFrame) == 0) {
    return(FALSE)
  }
  
  # Drop table if exists
  DatabaseConnector::renderTranslateExecuteSql(connection, "DROP TABLE IF EXISTS #cse_cohort_tags;")
  
  # Create temporary table using DatabaseConnector
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = "cse_cohort_tags",
    data = tagsDataFrame,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = TRUE,
    camelCaseToSnakeCase = TRUE
  )
  
  return(TRUE)
}


#' Convert old targetCohortIds format to new cohortTags format
#' @description
#' Provides backward compatibility by converting simple cohort ID vector to tag format
#' @param targetCohortIds Integer vector of cohort IDs
#' @returns Named list with single "default" tag containing the cohort IDs
#' @keywords internal
convertTargetCohortIdsToTags <- function(targetCohortIds) {
  if (is.null(targetCohortIds)) {
    return(NULL)
  }
  
  return(list("default" = as.integer(targetCohortIds)))
}
