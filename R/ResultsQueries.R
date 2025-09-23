#' Get Cohort Definitions
#'
#' Queries the database for available cohort definitions.
#'
#' @param qns A QueryNamespace object.
#'
#' @return A data.frame of cohort definitions.
#'
#' @examples
#' # qns <- createResultsQueryNamespace(...)
#' # getCohortDefinitions(qns)
getCohortDefinitions <- function(qns) {
  checkmate::assertClass(qns, "QueryNamespace")
  qns$queryDb("select distinct
               t.cohort_definition_id,
               short_name,
               atc_flag as is_atc
             from @schema.@cohort_definition t
             where t.cohort_definition_id is not null
             --and   atc_flag in (0, 1)
             order by short_name")
}

#' Get Data Source Table for All Databases
#'
#' Retrieves metadata about all database sources, optionally as a Reactable table.
#'
#' @param qns A QueryNamespace object.
#' @param reactableTable Logical. If TRUE (default), returns a Reactable table; otherwise returns a data.frame.
#'
#' @return A Reactable table or data.frame of database sources.
#'
#' @examples
#' # getDbDataSourcesTable(qns)
getDbDataSourcesTable <- function(qns, reactableTable = TRUE) {
  checkmate::assertClass(qns, "QueryNamespace")
  dataSourceData <- qns$queryDb("select
              cdm_source_abbreviation,
              cdm_holder,
              source_description,
              cdm_version,
              vocabulary_version,
              source_release_date
      from @schema.@cdm_source_info t")

  if (reactableTable) {
    colnames(dataSourceData) <- SqlRender::camelCaseToTitleCase(colnames(dataSourceData))
    rt <- reactable::reactable(
      data = dataSourceData,
      columns = list(
        "Source Description" = reactable::colDef(
          minWidth = 300)),
      defaultPageSize = 5
    )
    return(rt)
  }

  return(dataSourceData)
}

#' Get Co-occurrence Table Data
#'
#' Retrieves covariate data for covariates that occur on the same day as the exposure event.
#'
#' @param qns A QueryNamespace object.
#' @param databaseIds Character vector of database IDs.
#' @param prevInputHighMax Numeric. High maximum prevalence threshold.
#' @param prevInputHighMin Numeric. High minimum prevalence threshold.
#' @param prevInputLowMax Numeric. Low maximum prevalence threshold.
#' @param prevInputLowMin Numeric. Low minimum prevalence threshold.
#' @param cohortDefinitionId1 Target cohort definition ID.
#' @param cohortDefinitionId2 Comparator cohort definition ID.
#'
#' @return A data.frame with co-occurrence covariate data.
#'
#' @examples
#' # getCoOccurenceTableData(qns, ...)
getCoOccurenceTableData <- function(qns,
                                    databaseIds,
                                    prevInputHighMax,
                                    prevInputHighMin,
                                    prevInputLowMax,
                                    prevInputLowMin,
                                    cohortDefinitionId1,
                                    cohortDefinitionId2) {
  checkmate::assertClass(qns, "QueryNamespace")
  qns$queryDb(
    sql = "
            with means_cte as (
              	select
              	    c1.database_id,
              		@cohortDefinitionId1 as cohort_definition_id_1,
              		@cohortDefinitionId2 as cohort_definition_id_2,
              		case
              			when c1.covariate_type is null then c2.covariate_type
              			when c2.covariate_type is null then c1.covariate_type
              			else c1.covariate_type
              		end as covariate_type,
              		case
              			when c1.covariate_id is null then c2.covariate_id
              			when c2.covariate_id is null then c1.covariate_id
              			else c1.covariate_id
              		end as covariate_id,
              		case
              			when c1.covariate_name is null then c2.covariate_name
              			when c2.covariate_name is null then c1.covariate_name
              			else c1.covariate_name
              		end as covariate_short_name,
              		case
              			when c1.covariate_mean is null then 0.0
              			else c1.covariate_mean
              		end as mean_1,
              		case
              			when c2.covariate_mean is null then 0.0
              			else c2.covariate_mean
              		end as mean_2
              	from (
              	  select t.*, covd.covariate_name, covd.covariate_type
              	  from @schema.@covariate_mean t
              	  inner join @schema.@covariate_definition covd on covd.covariate_id = t.covariate_id
              	  where t.cohort_definition_id = @cohortDefinitionId1
              	  and t.database_id IN (@database_ids)
            	  ) as c1
              	left join (
              	  select t.*, covd.covariate_name, covd.covariate_type
              	  from @schema.@covariate_mean t
              	  inner join @schema.@covariate_definition covd on covd.covariate_id = t.covariate_id
              	  where t.cohort_definition_id = @cohortDefinitionId2
              	  and t.database_id IN (@database_ids)
              	 ) as c2
              on c1.covariate_id = c2.covariate_id AND c1.database_id = c2.database_id
              WHERE (c1.covariate_type IS NULL OR c1.covariate_type = 'Co-occurrence')
              AND   (c2.covariate_type IS NULL OR c2.covariate_type = 'Co-occurrence')
            )

            select
              m.*,
              d.cdm_source_abbreviation,
              case
                  when m.mean_1 = m.mean_2 then 0.0
                  when m.mean_1 = 0.0 and m.mean_2 = 1.0 then null
                  when m.mean_1 = 1.0 and m.mean_2 = 0.0 then null
                  else (mean_1 - mean_2) / (sqrt((mean_1 * (1 - mean_1) + mean_2 * (1 - mean_2)) / 2))
              end as std_diff,
            c1.num_persons as n_1,
            c2.num_persons as n_2
            from means_cte as m
            inner join @schema.@cohort_count as c1
            on m.cohort_definition_id_1 = c1.cohort_definition_id and c1.database_id = m.database_id
            inner join @schema.@cohort_count as c2
              on m.cohort_definition_id_2 = c2.cohort_definition_id and c2.database_id = m.database_id
            inner join @schema.@cdm_source_info as d on d.database_id = m.database_id

            WHERE (
              (m.mean_1 > @prevInputHighMax AND m.mean_2 < @prevInputHighMin) OR
              (m.mean_2 > @prevInputHighMax AND m.mean_1 < @prevInputHighMin)
            ) OR (
              (m.mean_1 > @prevInputLowMax AND m.mean_2 < @prevInputLowMin) OR
              (m.mean_2 > @prevInputLowMax AND m.mean_1 < @prevInputLowMin)
            )
              ;",
    database_ids = databaseIds,
    prevInputHighMax = prevInputHighMax,
    prevInputHighMin = prevInputHighMin,
    prevInputLowMax = prevInputLowMax,
    prevInputLowMin = prevInputLowMin,
    cohortDefinitionId1 = cohortDefinitionId1,
    cohortDefinitionId2 = cohortDefinitionId2)
}

#' Get Cohort Definitions Table for a Database
#'
#' Returns a table of cohort definitions and counts for a specified database.
#'
#' @param qns A QueryNamespace object.
#' @param databaseId Character or integer. The database ID(s) to fetch data for.
#' @param counts Logical. Whether to include counts (default TRUE).
#'
#' @return A data.frame of cohort definitions and counts.
#'
#' @examples
#' # getCohortDefinitionsTable(qns, databaseId = "CCAE")
getCohortDefinitionsTable <- function(qns, databaseId, counts = TRUE) {
  qns$queryDb(
    sql = "select distinct
               t.cohort_definition_id,
               short_name,
               atc_flag as is_atc,
               c.num_persons,
               c.database_id
             from @schema.@cohort_definition t
             inner join @schema.@cohort_count c ON c.cohort_definition_id = t.cohort_definition_id
             where t.cohort_definition_id is not null
             -- and   atc_flag in (0, 1)
             and c.database_id IN (@database_id)
             order by short_name",
    database_id = databaseId,
    counts = counts
  )
}

#' Get Pairwise Covariate Data
#'
#' Retrieves covariate mean and standardized difference for a pair of cohorts in a database.
#'
#' @param qns A QueryNamespace object.
#' @param databaseId Character or integer. The database ID.
#' @param cohortDefinitionId1 Target cohort definition ID.
#' @param cohortDefinitionId2 Comparator cohort definition ID.
#'
#' @return A data.frame with covariate means and std. differences.
#'
#' @examples
#' # getPairwiseCovariateData(qns, "CCAE", 101, 202)
getPairwiseCovariateData <- function(qns, databaseId, cohortDefinitionId1, cohortDefinitionId2) {
  checkmate::assertClass(qns, "QueryNamespace")
  qns$queryDb(
    sql = "with means as (
              	select
              		@cohortDefinitionId1 as cohort_definition_id_1,
              		@cohortDefinitionId2 as cohort_definition_id_2,
              		case
              			when c1.covariate_type is null then c2.covariate_type
              			when c2.covariate_type is null then c1.covariate_type
              			else c1.covariate_type
              		end as covariate_type,
              		case
              			when c1.covariate_id is null then c2.covariate_id
              			when c2.covariate_id is null then c1.covariate_id
              			else c1.covariate_id
              		end as covariate_id,
              		case
              			when c1.covariate_name is null then c2.covariate_name
              			when c2.covariate_name is null then c1.covariate_name
              			else c1.covariate_name
              		end as covariate_short_name,
              		case
              			when c1.covariate_mean is null then 0.0
              			else c1.covariate_mean
              		end as mean_1,
              		case
              			when c2.covariate_mean is null then 0.0
              			else c2.covariate_mean
              		end as mean_2
              	from (
              	  select t.*, covd.covariate_name, covd.covariate_type from @schema.@covariate_mean t
              	  inner join @schema.@covariate_definition covd on covd.covariate_id = t.covariate_id
              	  where t.cohort_definition_id = @cohortDefinitionId1
              	  and t.database_id = @database_id
            	  ) as c1
              	full join (
              	  select t.*, covd.covariate_name, covd.covariate_type from @schema.@covariate_mean t
              	  inner join @schema.@covariate_definition covd on covd.covariate_id = t.covariate_id
              	   where t.cohort_definition_id = @cohortDefinitionId2
              	   and t.database_id = @database_id
              	 ) as c2
              on c1.covariate_id = c2.covariate_id)
              select
              	m.*,
              	case
              		when m.mean_1 = m.mean_2 then 0.0
              		when m.mean_1 = 0.0 and m.mean_2 = 1.0 then null
              		when m.mean_1 = 1.0 and m.mean_2 = 0.0 then null
              		else (mean_1 - mean_2) / (sqrt((mean_1 * (1 - mean_1) + mean_2 * (1 - mean_2)) / 2))
              	end as std_diff,
              c1.num_persons as n_1,
              c2.num_persons as n_2
              from means as m
              join @schema.@cohort_count as c1
              	on m.cohort_definition_id_1 = c1.cohort_definition_id and c1.database_id = @database_id
              join @schema.@cohort_count as c2
              	on m.cohort_definition_id_2 = c2.cohort_definition_id and c2.database_id = @database_id
              ;",
    database_id = databaseId,
    cohortDefinitionId1 = cohortDefinitionId1,
    cohortDefinitionId2 = cohortDefinitionId2)
}

#' Get Cohort Similarity Scores
#'
#' Retrieves similarity scores for all comparators to a target cohort.
#'
#' @param qns A QueryNamespace object.
#' @param targetCohortId Integer or character. The target cohort definition ID.
#' @param weights optional named vector of numeric weights Demographics, "Medical history" "Presentation" "prior meds" "visit context"
#' with numeric values to adjust
#'
#' @return A data.frame with similarity scores.
#'
#' @examples
#' # getCohortSimilarityScores(qns, 101)
getCohortSimilarityScores <- function(qns, targetCohortId, weights = NULL) {
  checkmate::assertClass(qns, "QueryNamespace")
  checkmate::assertNumber(targetCohortId)

  if (is.null(weights)) {
    demographicsWeight <- .2
    historyWeight <- .2
    presentationWeight <- .2
    medsWeight <- .2
    visitWeight <- .2
  } else {
    demographicsWeight <- weights["Demographics"]
    historyWeight <- weights["Medical history"]
    presentationWeight <- weights["Presentation"]
    medsWeight <- weights["prior meds"]
    visitWeight <- weights["visit context"]
  }
  if (any(is.na(c(demographicsWeight, historyWeight, presentationWeight,
                  medsWeight, visitWeight)))) {
    demographicsWeight <- .2
    historyWeight <- .2
    presentationWeight <- .2
    medsWeight <- .2
    visitWeight <- .2
  }


  qns$queryDb(
    sql = "
            select
              database_id,
              cdm_source_abbreviation,
              cohort_definition_id_2,
              is_atc_2,
              short_name,
              sum(cosine_similarity * weight) as cosine_similarity,
              atc_4_related,
              atc_3_related,
              num_persons
            from (
              select distinct
                csi.database_id,
                csi.cdm_source_abbreviation,
                t.covariate_type,
              case
                        when t.covariate_type = 'Demographics' then @demographicsWeight
                        when t.covariate_type = 'Medical history' then @historyWeight
                        when t.covariate_type = 'Presentation' then @presentationWeight
                        when t.covariate_type = 'prior meds' then @medsWeight
                        when t.covariate_type = 'visit context' then @visitWeight
                    else 0.0
              end as weight,

              case
                    when t.cohort_definition_id_1 = @targetCohortId then t.cohort_definition_id_2
                    else t.cohort_definition_id_1
              end as cohort_definition_id_2,

              case
                    when t.cohort_definition_id_1 = @targetCohortId then cd2.atc_flag
                    else cd.atc_flag
              end as is_atc_2,

              case  when t.cohort_definition_id_1 = @targetCohortId then cd2.short_name
                    else cd.short_name
              end as short_name,
              cosine_similarity,
              atc.atc_4_related,
              atc.atc_3_related,

              case
                    when t.cohort_definition_id_1 = @targetCohortId then ec.num_persons
                    else ec2.num_persons
              end as num_persons

              from @schema.@cosine_similarity_score  t
	              inner join @schema.@cohort_count ec ON ec.cohort_definition_id = t.cohort_definition_id_2
	                  and ec.database_id = t.database_id
	              inner join @schema.@cohort_count ec2 ON ec2.cohort_definition_id = t.cohort_definition_id_1
	                  and ec2.database_id = t.database_id
	              inner join @schema.@cdm_source_info csi ON csi.database_id = t.database_id
	              inner join @schema.@cohort_definition cd ON cd.cohort_definition_id = t.cohort_definition_id_1
	              inner join @schema.@cohort_definition cd2 ON cd2.cohort_definition_id = t.cohort_definition_id_2
	              left join @schema.@atc_level atc on (t.cohort_definition_id_1 = atc.cohort_definition_id_1
	                  and t.cohort_definition_id_2 = atc.cohort_definition_id_2)
	                  or (t.cohort_definition_id_2 = atc.cohort_definition_id_1
	                  and t.cohort_definition_id_1 = atc.cohort_definition_id_2)
	              where (t.cohort_definition_id_1 = @targetCohortId or t.cohort_definition_id_2 = @targetCohortId)
	              and t.covariate_type not in  ('average', 'Co-occurrence')
	          ) domains
	        group by database_id, cdm_source_abbreviation, cohort_definition_id_2, is_atc_2, short_name,
	             atc_4_related, atc_3_related, num_persons
	        order by database_id, cdm_source_abbreviation, cosine_similarity desc
          ", targetCohortId = targetCohortId,
    demographicsWeight = demographicsWeight,
    historyWeight = historyWeight,
    presentationWeight = presentationWeight,
    medsWeight = medsWeight,
    visitWeight = visitWeight

  )
}

#' Get Database Similarity Scores
#'
#' Retrieves similarity scores for a target cohort across specific databases.
#'
#' @param qns A QueryNamespace object.
#' @param targetCohortId Integer or character. The target cohort definition ID.
#' @param databaseIds Character vector of database IDs.
#'
#' @return A data.frame with similarity scores for the specified databases.
#'
#' @examples
#' # getDatabaseSimilarityScores(qns, 101, c("CCAE", "MDCR"))
getDatabaseSimilarityScores <- function(qns, targetCohortId, databaseIds) {
  checkmate::assertClass(qns, "QueryNamespace")
  qns$queryDb(
    sql = "
            select distinct
               d.cdm_source_abbreviation,
               CASE
                  WHEN t.cohort_definition_id_1 = @targetCohortId THEN t.cohort_definition_id_2
                  ELSE t.cohort_definition_id_1
               END as cohort_definition_id_2,

               CASE
                  WHEN t.cohort_definition_id_1 = @targetCohortId THEN cd2.atc_flag
                  ELSE cd.atc_flag
               END as is_atc_2,

               CASE
                  WHEN t.cohort_definition_id_1 = @targetCohortId THEN cd2.short_name
                  ELSE cd.short_name
               END as short_name,
               cosine_similarity,
               CASE
                  WHEN t.cohort_definition_id_1 = @targetCohortId THEN ec.num_persons
                  ELSE ec2.num_persons
               END as num_persons,
               t.covariate_type
             from @schema.@cosine_similarity_score t
             inner join @schema.@cohort_count ec ON ec.cohort_definition_id = t.cohort_definition_id_2 and ec.database_id = t.database_id
             inner join @schema.@cohort_count ec2 ON ec2.cohort_definition_id = t.cohort_definition_id_1 and ec2.database_id = t.database_id
             inner join @schema.@cohort_definition cd ON cd.cohort_definition_id = t.cohort_definition_id_1
             inner join @schema.@cohort_definition cd2 ON cd2.cohort_definition_id = t.cohort_definition_id_2
             inner join @schema.@cdm_source_info d ON t.database_id = d.database_id
             where (t.cohort_definition_id_1 = @targetCohortId or t.cohort_definition_id_2 = @targetCohortId)
             and t.database_id IN (@database_ids)
           ",
    targetCohortId = targetCohortId,
    database_ids = databaseIds)
}

#' Get Cosine Similarity Table for a Database
#'
#' Retrieves cosine similarity values for a target and comparator cohort in a specific database.
#'
#' @param qns A QueryNamespace object.
#' @param targetCohortId Target cohort definition ID.
#' @param comparatorCohortId Comparator cohort definition ID.
#' @param databaseId Character or integer. The database ID.
#' @param returnReactable Logical. If TRUE, returns a Reactable table; otherwise returns a data.frame.
#' @param weights a set of numeric weights > 0 to scale each covariate domain in the cosine similarity calculation (optional)
#' @param fmtSim  number of signficant figures to format float output
#' @return A Reactable table or data.frame of cosine similarity values.
#'
#' @examples
#' # getDbCosineSimilarityTable(qns, 101, 202, "CCAE", TRUE)
getDbCosineSimilarityTable <- function(qns, targetCohortId, comparatorCohortId, databaseId, weights = NULL, returnReactable = FALSE, fmtSim = "%.3f") {


  checkmate::assertClass(qns, "QueryNamespace")
  sql <- "SELECT covariate_type, cosine_similarity FROM @schema.@cosine_similarity_score
    WHERE database_id = @database_id
    AND cohort_definition_id_1 in (@target, @comparator)
    AND cohort_definition_id_2 in (@target, @comparator)
    "
  detailData <- qns$queryDb(sql,
                            database_id = databaseId,
                            target = targetCohortId,
                            comparator = comparatorCohortId)

  showWeights <- !is.null(weights)

  if (showWeights) {
    # Apply weights to matching domain names
    detailData <- detailData %>%
      dplyr::filter(.data$covariateType != "average") %>%
      dplyr::mutate(
        weight = dplyr::case_when(
          .data$covariateType == "Demographics" ~ weights["Demographics"],
          .data$covariateType == "Presentation" ~ weights["Presentation"],
          .data$covariateType == "Medical history" ~ weights["Medical history"],
          .data$covariateType == "prior meds" ~ weights["prior meds"],
          .data$covariateType == "visit context" ~ weights["visit context"],
          TRUE ~ 0
        )
      )

    # Final row with total weighted similarity
    weightedTotal <- sum(detailData$cosineSimilarity * detailData$weight, na.rm = TRUE)

    detailData <- dplyr::bind_rows(
      detailData,
      data.frame(
        covariateType = "Weighted Similarity Score",
        cosineSimilarity = weightedTotal,
        weight = NA
      )
    )
  } else {
    detailData <- detailData %>%
      dplyr::filter(.data$covariateType != "average")
  }

  if (returnReactable) {
    if (showWeights) {
      columnsList <- list(
        covariateType = reactable::colDef(name = "Covariate Domain"),
        cosineSimilarity = reactable::colDef(
          name = "Raw Similarity Score",
          cell = function(value, index) {
            if (detailData$covariateType[index] == "Weighted Similarity Score") {
              htmltools::strong(sprintf(fmtSim, value))
            } else {
              sprintf(fmtSim, value)
            }
          }
        ),
        weight = reactable::colDef(
          name = "User Weight",
          cell = function(value) {
            if (is.na(value)) "" else paste0(round(value * 100), "%")
          }
        )
      )
    } else {
      columnsList <- list(
        covariateType = reactable::colDef(name = "Covariate Domain"),
        cosineSimilarity = reactable::colDef(
          name = "Similarity Score",
          cell = function(value, index) {
            if (detailData$covariateType[index] == "Weighted Similarity Score") {
              htmltools::strong(sprintf(fmtSim, value))
            } else {
              sprintf(fmtSim, value)
            }
          }
        )
      )
    }

    return(
      reactable::reactable(
        data = detailData,
        columns = columnsList
      )
    )
  }

  return(detailData)
}

#' Get All Database Sources
#'
#' Retrieves all database source metadata from the results schema.
#'
#' @param qns A QueryNamespace object.
#'
#' @return A data.frame of all database sources.
#'
#' @examples
#' # getDatabaseSources(qns)
getDatabaseSources <- function(qns) {
  checkmate::assertClass(qns, "QueryNamespace")
  qns$queryDb(sql = "select distinct * from @schema.@cdm_source_info t")
}


#' Create Query Namespace for Results Data Model
#'
#' This function initializes a query namespace object for use with the Results data model.
#' It loads the data model specification from a CSV file and calls
#' \code{ResultModelManager::createQueryNamespace()} with the provided connection and schema info.
#'
#' @param connectionDetails A DatabaseConnector connection details object.
#' @param resultsSchema The database schema where the results tables are located.
#' @param tablePrefix Optional table prefix for the results tables (default is \code{""}).
#' @param dataModelSpecPath Path to the results data model specification CSV file (default is \code{"resultsDataModel.csv"}).
#' @param usePooledConnection Logical; whether to use pooled connections (default is \code{FALSE}).
#'
#' @return A query namespace object as returned by \code{ResultModelManager::createQueryNamespace()}.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql", ...)
#' qns <- createResultsQueryNamespace(
#'   connectionDetails = connectionDetails,
#'   resultsSchema = "my_results_schema"
#' )
#' }
createResultsQueryNamespace <- function(
  connectionDetails,
  resultsSchema,
  tablePrefix = "",
  dataModelSpecPath = system.file("settings", "resultsDataModel.csv", package = "ComparatorSelectionExplorer"),
  usePooledConnection = FALSE
) {
  dataModelSpec <- ResultModelManager::loadResultsDataModelSpecifications(dataModelSpecPath)
  qns <- ResultModelManager::createQueryNamespace(
    connectionDetails = connectionDetails,
    usePooledConnection = usePooledConnection,
    schema = resultsSchema,
    tablePrefix = tablePrefix,
    tableSpecification = dataModelSpec
  )
  return(qns)
}