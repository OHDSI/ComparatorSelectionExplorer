# Copyright 2022 Observational Health Data Sciences and Informatics
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
  on.exit(migrator$finalize(), add = TRUE)
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
#'
#' Upload results to database server
#'
#' @param connectionDetails             DatabaseConnector connection details object
#' @param databaseSchema                String schema where database schema lives
#' @param tablePrefix                  (Optional) Use if a table prefix is used before table names (e.g. "cd_")
#' @param zipFileName                  Path to zipFile containing results
#' @param importFilpath                file path to export zipped results to before upload (optional - default is temporary)
#' @param ...                          Elipsis  - see ResultModelManager::uploadResults
#' @export
uploadResults <- function(connectionDetails, databaseSchema, zipFileName, tablePrefix = "", importFilpath = tempfile(), ...) {

  if (!dir.exists(importFilpath)) {
    dir.create(importFilpath)
  }

  ResultModelManager::unzipResults(zipFileName, importFilpath)

  if (connectionDetails$dbms == "postgresql") {
    # this would be much cleaner with a trigger on insert to cdm_source_info table
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection), add = TRUE)

    sql <- "
    CREATE TABLE IF NOT EXISTS @database_schema.@table_prefixcosine_similarity_@database_id
    PARTITION OF @database_schema.@table_prefixcosine_similarity_score FOR VALUES IN (@database_id);

    CREATE TABLE IF NOT EXISTS @database_schema.@table_prefixcovariate_mean_@database_id
    PARTITION OF @database_schema.@table_prefixcovariate_mean FOR VALUES IN (@database_id);
    "

    sourceInfo <- readr::read_csv(file.path(importFilpath, "cdm_source_info.csv"),
                                  show_col_types = FALSE)
    databaseIds <- unique(sourceInfo$database_id)

    for (databaseId in databaseIds) {
      DatabaseConnector::renderTranslateExecuteSql(connection,
                                                   sql,
                                                   database_schema = databaseSchema,
                                                   database_id = databaseId,
                                                   table_prefix = tablePrefix)
    }
  }
  ResultModelManager::uploadResults(connectionDetails = connectionDetails,
                                    schema = databaseSchema,
                                    resultsFolder = importFilpath,
                                    tablePrefix = tablePrefix,
                                    specifications = getResultsDataModelSpec(),
                                    ...)
}
