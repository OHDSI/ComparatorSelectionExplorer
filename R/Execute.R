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


#' @title
#' Execute Package
#' @description
#' Execute data generation step on a given OMOP CDM - connect, create cohorts and generate data
#' Data can be transferred with the OhdsiSharing package
#'
#' @param executionSettings   settings greated with @seealso createExecutionSettings
#' @param ...                 settings greated with @seealso createExecutionSettings
#' @export
execute <- function(executionSettings = NULL, ...) {

  if (is.null(executionSettings) || missing(executionSettings)) {
    executionSettings <- createExecutionSettings(...)
  }

  executionSettings <- generateSimilarityScores(executionSettings)
  executionSettings <- exportResults(executionSettings)

  if (isTRUE(executionSettings$connectionInternallyCreated) && !is.null(executionSettings$connection)) {
    DatabaseConnector::disconnect(executionSettings$connection)
    executionSettings$connection <- NULL
  }

  invisible(executionSettings)
}
