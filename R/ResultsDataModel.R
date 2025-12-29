# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of CohortGenerator
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


#' Migrate Data model
#' @description
#' Migrate data from current state to next state
#'
#' It is strongly advised that you have a backup of all data (either sqlite files, a backup database (in the case you
#' are using a postgres backend) or have kept the csv/zip files from your data generation.
#'
#' @inheritParams getDataMigrator
#' @export
migrateDataModel <- function(connectionDetails, databaseSchema, tablePrefix = "") {
  ParallelLogger::logInfo("Migrating data set")
  migrator <- getDataMigrator(connectionDetails = connectionDetails, databaseSchema = databaseSchema, tablePrefix = tablePrefix)
  on.exit(migrator$closeConnection(), add = TRUE)
  migrator$executeMigrations()

  ParallelLogger::logInfo("Updating version number")
  updateVersionSql <- SqlRender::loadRenderTranslateSql("UpdateVersionNumber.sql",
                                                        packageName = utils::packageName(),
                                                        database_schema = databaseSchema,
                                                        table_prefix = tablePrefix,
                                                        dbms = connectionDetails$dbms)
  migrator$getConnectionHandler()$executeSql(updateVersionSql)
}


#' Get database migrations instance
#' @description
#'
#' Returns ResultModelManager DataMigrationsManager instance.
# '@seealso [ResultModelManager::DataMigrationManager] which this function is a utility for.
#'
#' @param connectionDetails             DatabaseConnector connection details object
#' @param databaseSchema                String schema where database schema lives
#' @param  tablePrefix                  (Optional) Use if a table prefix is used before table names (e.g. "cd_")
#' @returns Instance of ResultModelManager::DataMigrationManager that has interface for converting existing data models
#' @export
getDataMigrator <- function(connectionDetails, databaseSchema, tablePrefix = "") {
  ResultModelManager::DataMigrationManager$new(connectionDetails = connectionDetails,
                                               databaseSchema = databaseSchema,
                                               tablePrefix = tablePrefix,
                                               packageTablePrefix = "cse_",
                                               migrationPath = "migrations",
                                               packageName = utils::packageName())
}

#' Create the results data model tables on a database server.
#'
#' @details
#' Only PostgreSQL servers are supported.
#' @inheritParams getDataMigrator
#' @export
createResultsDataModel <- function(connectionDetails, databaseSchema, tablePrefix = "") {
  migrateDataModel(connectionDetails, databaseSchema, tablePrefix)
}

#'Get Results Data Model Specifcations
#'
#' @export
getResultsDataModelSpec <- function() {
  specPath <- system.file("settings", "resultsDataModel.csv", package = utils::packageName())
  spec <- readr::read_csv(specPath, show_col_types = FALSE)
  colnames(spec) <- SqlRender::snakeCaseToCamelCase(colnames(spec))
  return(spec)
}

#' Upload Results
#' @description
#' Upload results to a database server from either a zip file or a pre-extracted folder.
#'
#' @param connectionDetails  DatabaseConnector connection details object
#' @param databaseSchema     String schema where database schema lives
#' @param tablePrefix        (Optional) Use if a table prefix is used before table names (e.g. "cd_")
#' @param zipFileName        Path to zip file containing results (optional if importFilePath is given)
#' @param importFilePath     Path to already-extracted results folder (optional if zipFileName is given)
#' @param ...                Additional parameters passed to ResultModelManager::uploadResults
#'
#' @export
uploadResults <- function(connectionDetails,
                           databaseSchema,
                           zipFileName = NULL,
                           importFilePath = NULL,
                           tablePrefix = "",
                           ...) {

  # --- Validate inputs ---
  if (is.null(zipFileName) && is.null(importFilePath)) {
    stop("You must specify either 'zipFileName' or 'importFilePath'.", call. = FALSE)
  }

  # --- If zipFile is provided, unzip into a folder ---
  if (!is.null(zipFileName)) {
    if (!file.exists(zipFileName)) {
      stop("Zip file does not exist: ", zipFileName)
    }
    # If no importFilePath specified, create a temp dir
    if (is.null(importFilePath)) {
      importFilePath <- tempfile()
    }
    if (!dir.exists(importFilePath)) {
      dir.create(importFilePath, recursive = TRUE)
    }
    ResultModelManager::unzipResults(zipFileName, importFilePath)
  }

  # --- If only folder is provided, ensure it exists ---
  if (!is.null(importFilePath) && !dir.exists(importFilePath)) {
    stop("The specified importFilePath does not exist: ", importFilePath)
  }

  # --- Special handling for PostgreSQL partition creation ---
  if (connectionDetails$dbms == "postgresql") {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection), add = TRUE)

    # Covariate types to create as subpartitions
    covariateTypes <- c("average", "Medical history", "Presentation", "Demographics", "prior meds", "visit context")

    # Create top-level partitions per database_id
    sqlTop <- "
      CREATE TABLE IF NOT EXISTS @database_schema.@table_prefixcse_cosine_similarity_@database_id
      PARTITION OF @database_schema.@table_prefixcse_cosine_similarity_score
      FOR VALUES IN (@database_id)
      PARTITION BY LIST (covariate_type);

      CREATE TABLE IF NOT EXISTS @database_schema.@table_prefixcse_covariate_mean_@database_id
      PARTITION OF @database_schema.@table_prefixcse_covariate_mean
      FOR VALUES IN (@database_id);
    "

    # Create subpartitions for each covariate_type
    sqlSub <- "
      CREATE TABLE IF NOT EXISTS @database_schema.@table_prefixcse_cosine_similarity_@database_id_@covariate_slug
      PARTITION OF @database_schema.@table_prefixcse_cosine_similarity_@database_id
      FOR VALUES IN (@covariate_literal);
    "

    sourceInfo <- readr::read_csv(
      file.path(importFilePath, "cse_cdm_source_info.csv"),
      show_col_types = FALSE
    )
    databaseIds <- unique(sourceInfo$database_id)

    for (databaseId in databaseIds) {
      # Create top-level partitions
      DatabaseConnector::renderTranslateExecuteSql(
        connection,
        sqlTop,
        database_schema = databaseSchema,
        database_id = databaseId,
        table_prefix = tablePrefix
      )

      # Create subpartitions
      for (covType in covariateTypes) {
        # Create slug for table names (replace spaces with underscores)
        covSlug <- gsub(" ", "_", tolower(covType))

        DatabaseConnector::renderTranslateExecuteSql(
          connection,
          sqlSub,
          database_schema = databaseSchema,
          database_id = databaseId,
          table_prefix = tablePrefix,
          covariate_slug = covSlug,
          covariate_literal = covType
        )
      }
    }
  }

  # --- Upload results ---
  ResultModelManager::uploadResults(
    connectionDetails = connectionDetails,
    schema = databaseSchema,
    resultsFolder = importFilePath,
    tablePrefix = tablePrefix,
    databaseIdentifierFile = "cse_cdm_source_info.csv",
    specifications = getResultsDataModelSpec(),
    ...
  )
}