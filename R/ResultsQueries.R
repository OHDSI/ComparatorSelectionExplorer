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
               cohort_name as short_name,
               coalesce(tag, '0') = 'ATC' as is_atc
             from @schema.@cg_cohort_definition t
             left join @schema.@cse_cohort_tag ct on t.cohort_definition_id = ct.cohort_definition_id AND tag = 'ATC'
             where t.cohort_definition_id is not null
             order by cohort_name")
}


#' Get Cohort Tags
#' @description
#' Get any tags assigned to cohorts
#' @param qns a query namespace object
#' @param cohortIds optional cohort ids
getCohortTags <- function(qns, cohortIds = '') {
  qns$queryDb("
    SELECT DISTINCT tag FROM @schema.@cse_cohort_tag
    {@cohort_ids != ''} ? {WHERE cohort_definition_id IN (@cohort_ids)}", cohort_ids = cohortIds
  )
}

#' Get Cohorts by tag id
#' @description
#' Get any tags assigned to cohorts
#' @param qns a query namespace object
#' @param cohortIds optional cohort ids
getCohortsByTag <- function(qns, tag) {
  safe_tag <- gsub("'", "''", tag)  # escape single quotes
  sql <-"
    SELECT
      t.cohort_definition_id,
      cohort_name AS short_name
    FROM @schema.@cg_cohort_definition t
    INNER JOIN @schema.@cse_cohort_tag ct
      ON t.cohort_definition_id = ct.cohort_definition_id
    INNER JOIN @schema.@cse_cohort_count cc
      ON t.cohort_definition_id = cc.cohort_definition_id
    WHERE ct.tag = '@safe_tag'
    GROUP BY t.cohort_definition_id, cohort_name
    HAVING MAX(cc.num_persons) > 0
   "
  qns$queryDb(sql, safe_tag = safe_tag)
}

#' Get Cohorts by Tag with Counts
#' @description
#' Get cohorts by tag with their counts across all databases (only cohorts with count > 0)
#' @param qns a query namespace object
#' @param tag the tag to filter by
getCohortsByTagWithCounts <- function(qns, tag) {
  safe_tag <- gsub("'", "''", tag)  # escape single quotes
  sql <- "
    SELECT
      t.cohort_definition_id,
      cohort_name AS short_name,
      SUM(cc.cohort_subjects) AS total_subjects,
      SUM(cc.cohort_entries) AS total_entries,
      COUNT(DISTINCT cc.database_id) AS num_databases
    FROM @schema.@cg_cohort_definition t
    INNER JOIN @schema.@cse_cohort_tag ct
      ON t.cohort_definition_id = ct.cohort_definition_id
    INNER JOIN @schema.@cg_cohort_count cc
      ON t.cohort_definition_id = cc.cohort_id
    WHERE ct.tag = '@safe_tag'
      AND cc.cohort_subjects > 0
    GROUP BY t.cohort_definition_id, cohort_name
    ORDER BY cohort_name
  "
  qns$queryDb(sql, safe_tag = safe_tag)
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
      from @schema.@cse_cdm_source_info t")

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
              	  from @schema.@cse_covariate_mean t
              	  inner join @schema.@cse_covariate_definition covd on covd.covariate_id = t.covariate_id
              	  where t.cohort_definition_id = @cohortDefinitionId1
              	  and t.database_id IN (@database_ids)
            	  ) as c1
              	left join (
              	  select t.*, covd.covariate_name, covd.covariate_type
              	  from @schema.@cse_covariate_mean t
              	  inner join @schema.@cse_covariate_definition covd on covd.covariate_id = t.covariate_id
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
            inner join @schema.@cse_cohort_count as c1
            on m.cohort_definition_id_1 = c1.cohort_definition_id and c1.database_id = m.database_id
            inner join @schema.@cse_cohort_count as c2
              on m.cohort_definition_id_2 = c2.cohort_definition_id and c2.database_id = m.database_id
            inner join @schema.@cse_cdm_source_info as d on d.database_id = m.database_id

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
    sql = "
    select distinct
               t.cohort_definition_id,
               cohort_name as short_name,
               coalesce(tag, 'RxNorm') = 'ATC' as is_atc,
               c.num_persons,
               c.database_id
             from @schema.@cg_cohort_definition t
             inner join @schema.@cse_cohort_count c ON c.cohort_definition_id = t.cohort_definition_id
             left join @schema.@cse_cohort_tag ct on t.cohort_definition_id = ct.cohort_definition_id AND tag = 'ATC'
             where t.cohort_definition_id is not null
             and c.database_id IN (@database_id)
             order by cohort_name
    ",
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
              	  select t.*, covd.covariate_name, covd.covariate_type from @schema.@cse_covariate_mean t
              	  inner join @schema.@cse_covariate_definition covd on covd.covariate_id = t.covariate_id
              	  where t.cohort_definition_id = @cohortDefinitionId1
              	  and t.database_id = @database_id
            	  ) as c1
              	full join (
              	  select t.*, covd.covariate_name, covd.covariate_type from @schema.@cse_covariate_mean t
              	  inner join @schema.@cse_covariate_definition covd on covd.covariate_id = t.covariate_id
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
              join @schema.@cse_cohort_count as c1
              	on m.cohort_definition_id_1 = c1.cohort_definition_id and c1.database_id = @database_id
              join @schema.@cse_cohort_count as c2
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
                    when t.cohort_definition_id_1 = @targetCohortId then coalesce(ct2.tag, 'RxNorm') = 'ATC'
                    else coalesce(ct.tag, 'RxNorm') = 'ATC'
              end as is_atc_2,

              case  when t.cohort_definition_id_1 = @targetCohortId then cd2.cohort_name
                    else cd.cohort_name
              end as short_name,
              cosine_similarity,
              atc.atc_4_related,
              atc.atc_3_related,

              case
                    when t.cohort_definition_id_1 = @targetCohortId then ec.num_persons
                    else ec2.num_persons
              end as num_persons

              from @schema.@cse_cosine_similarity_score  t
	              inner join @schema.@cse_cohort_count ec ON ec.cohort_definition_id = t.cohort_definition_id_2
	                  and ec.database_id = t.database_id
	              inner join @schema.@cse_cohort_count ec2 ON ec2.cohort_definition_id = t.cohort_definition_id_1
	                  and ec2.database_id = t.database_id
	              inner join @schema.@cse_cdm_source_info csi ON csi.database_id = t.database_id
	              inner join @schema.@cg_cohort_definition cd ON cd.cohort_definition_id = t.cohort_definition_id_1
	              left join @schema.@cse_cohort_tag ct on t.cohort_definition_id_1 = ct.cohort_definition_id AND ct.tag = 'ATC'
	              inner join @schema.@cg_cohort_definition cd2 ON cd2.cohort_definition_id = t.cohort_definition_id_2
	              left join @schema.@cse_cohort_tag ct2 on t.cohort_definition_id_2 = ct.cohort_definition_id AND ct2.tag = 'ATC'
	              left join @schema.@cse_atc_level atc on (t.cohort_definition_id_1/1000 = atc.drug_concept_id_1
	                  and t.cohort_definition_id_2/1000 = atc.drug_concept_id_2)
	                  or (t.cohort_definition_id_2/1000 = atc.drug_concept_id_1
	                  and t.cohort_definition_id_1/1000 = atc.drug_concept_id_2)
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
                  WHEN t.cohort_definition_id_1 = @targetCohortId THEN coalesce(ct2.tag, '0') = 'ATC'
                  ELSE coalesce(ct.tag, '0') = 'ATC'
               END as is_atc_2,

               CASE
                  WHEN t.cohort_definition_id_1 = @targetCohortId THEN cd2.cohort_name
                  ELSE cd.cohort_name
               END as short_name,
               cosine_similarity,
               CASE
                  WHEN t.cohort_definition_id_1 = @targetCohortId THEN ec.num_persons
                  ELSE ec2.num_persons
               END as num_persons,
               t.covariate_type
             from @schema.@cse_cosine_similarity_score t
             inner join @schema.@cse_cohort_count ec ON ec.cohort_definition_id = t.cohort_definition_id_2 and ec.database_id = t.database_id
             inner join @schema.@cse_cohort_count ec2 ON ec2.cohort_definition_id = t.cohort_definition_id_1 and ec2.database_id = t.database_id
              inner join @schema.@cg_cohort_definition cd ON cd.cohort_definition_id = t.cohort_definition_id_1
              left join @schema.@cse_cohort_tag ct on t.cohort_definition_id_1 = ct.cohort_definition_id AND ct.tag = 'ATC'
              inner join @schema.@cg_cohort_definition cd2 ON cd2.cohort_definition_id = t.cohort_definition_id_2
              left join @schema.@cse_cohort_tag ct2 on t.cohort_definition_id_2 = ct.cohort_definition_id AND ct.tag = 'ATC'
             inner join @schema.@cse_cdm_source_info d ON t.database_id = d.database_id
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
  sql <- "SELECT covariate_type, cosine_similarity FROM @schema.@cse_cosine_similarity_score
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
  qns$queryDb(sql = "select distinct * from @schema.@cse_cdm_source_info t")
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
  dataModelSpec <- getResultsDataModelSpec() |>
    dplyr::bind_rows(CohortGenerator::getResultsDataModelSpecifications())

  qns <- ResultModelManager::createQueryNamespace(
    connectionDetails = connectionDetails,
    usePooledConnection = usePooledConnection,
    schema = resultsSchema,
    tablePrefix = tablePrefix,
    tableSpecification = dataModelSpec
  )
  return(qns)
}

#' Get Cohort Definitions for Cohort Generator Module
#'
#' Queries the cg_cohort_definition table for cohort definitions
#'
#' @param qns A QueryNamespace object.
#'
#' @return A data.frame of cohort definitions with id and name.
#'
#' @examples
#' \dontrun{
#' # qns <- createResultsQueryNamespace(...)
#' # getCohortGeneratorDefinitions(qns)
#' }
getCohortGeneratorDefinitions <- function(qns) {
  checkmate::assertClass(qns, "QueryNamespace")
  qns$queryDb("
    SELECT
      cohort_definition_id,
      cohort_name
    FROM @schema.@cg_cohort_definition
    ORDER BY cohort_name
  ")
}

#' Get Cohort Counts by Cohort Definition ID
#'
#' Queries the cg_cohort_count table for counts across databases for a specific cohort
#'
#' @param qns A QueryNamespace object.
#' @param cohortDefinitionId Cohort definition ID to get counts for.
#'
#' @return A data.frame of cohort counts by database.
#'
#' @examples
#' \dontrun{
#' # qns <- createResultsQueryNamespace(...)
#' # getCohortGeneratorCounts(qns, cohortDefinitionId = 123)
#' }
getCohortGeneratorCounts <- function(qns, cohortDefinitionId) {
  checkmate::assertClass(qns, "QueryNamespace")
  checkmate::assertIntegerish(cohortDefinitionId, len = 1)

  qns$queryDb("
    SELECT
      cc.database_id,
      ds.cdm_source_abbreviation,
      cc.cohort_entries,
      cc.cohort_subjects
    FROM @schema.@cg_cohort_count cc
    INNER JOIN @schema.@cse_cdm_source_info ds
      ON cc.database_id = ds.database_id
    WHERE cc.cohort_definition_id = @cohort_definition_id
    ORDER BY ds.cdm_source_abbreviation
  ", cohort_definition_id = cohortDefinitionId)
}

