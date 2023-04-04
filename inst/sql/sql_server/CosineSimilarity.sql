{DEFAULT @dotproduct_concept = ''}
{DEFAULT @vector_length = ''}
{DEFAULT @exclude_co_occurrence_average = TRUE}
/***
compute cosine similarity for all relavant cohort-cohort comparisons

***/

-- PARAMETERS
--- @cohort_counts: name of table where cohort sample sizes are stored
--- @cohort: name of cohort table to use for feature extraction
--- @cohort_definition: cohort definition table
--- @cdm_database_schema: CDM schema referenced by @results_database_schema
--- @results_database_schema: schema where cohort table is stored
--- @covariate_def_table: name of table where covariate defs are stored
--- @covariate_means_table: name of table where covariate means are stored
--- @cosine_sim_table_2: name of output table where cosine similarity (stratified by covariate domain) data is stored
--- @cosine_sim_table: name of output table where overall cosine similarity data is stored

--vector length for all cohorts, used in denominator of cos similarity calculation
drop table if exists #vector_length;
create table #vector_length as
select scs1.cohort_definition_id, scd1.covariate_type, sqrt(sum(scs1.covariate_mean * scs1.covariate_mean)) as vector_length
from @results_database_schema.@covariate_means_table scs1
  inner join @results_database_schema.@covariate_def_table scd1
  on scs1.covariate_id = scd1.covariate_id
group by scs1.cohort_definition_id, scd1.covariate_type
;

{@vector_length != ''} ? {
    DROP TABLE IF EXISTS @results_database_schema.@vector_length;
    CREATE TABLE @results_database_schema.@vector_length
    AS SELECT * FROM #vector_length;
}

--dotproduct for all cohort-cohort combinations, used in numerator of cos similarity calculation
--combinations to compare:  1) same database, same year, different concepts; 2) same database, same concept, different years,  3) (for when pooling across databases) different database, same concept, same year
drop table if exists #dotproduct_concept;
create table #dotproduct_concept as
select scs1.cohort_definition_id as cohort_definition_id_1, scs2.cohort_definition_id as cohort_definition_id_2,
scovd1.covariate_type, sum(scs1.covariate_mean*scs2.covariate_mean) as dotproduct
from
@results_database_schema.@covariate_means_table scs1
inner join @results_database_schema.@covariate_def_table scovd1
  on scs1.covariate_id = scovd1.covariate_id
inner join @results_database_schema.@cohort_definition scd1
on scs1.cohort_definition_id = scd1.cohort_definition_id
inner join @results_database_schema.@covariate_means_table scs2
on scs1.covariate_id = scs2.covariate_id
inner join @results_database_schema.@cohort_definition scd2
on scs2.cohort_definition_id = scd2.cohort_definition_id
where (scd1.concept_id  < scd2.concept_id and scd2.atc_flag in (0,1))
group by scs1.cohort_definition_id, scs2.cohort_definition_id, scovd1.covariate_type
;

{@dotproduct_concept != ''} ? {
    DROP TABLE IF EXISTS @results_database_schema.@dotproduct_concept;
    CREATE TABLE @results_database_schema.@dotproduct_concept
    AS SELECT * FROM #dotproduct_concept;
}

drop table if exists @results_database_schema.@cosine_sim_table_2;
create table @results_database_schema.@cosine_sim_table_2 as
select t1.cohort_definition_id_1, t1.cohort_definition_id_2, t1.covariate_type,
t1.dotproduct / (vl1.vector_length * vl2.vector_length) as cosine_similarity
from #dotproduct_concept t1
inner join #vector_length vl1 on (
    t1.cohort_definition_id_1 = vl1.cohort_definition_id and t1.covariate_type = vl1.covariate_type
)
inner join #vector_length vl2 on (
    t1.cohort_definition_id_2 = vl2.cohort_definition_id and t1.covariate_type = vl2.covariate_type
)
;

INSERT INTO @results_database_schema.@cosine_sim_table_2
select
    cohort_definition_id_1,
    cohort_definition_id_2,
    'average' as covariate_type,
    avg(cosine_similarity) as cosine_similarity
from @results_database_schema.@cosine_sim_table_2
{@exclude_co_occurrence_average} ? {WHERE covariate_type != 'Co-occurrence'}
group by cohort_definition_id_1, cohort_definition_id_2
;