-- PARAMETERS
--- @cohort_counts: name of table where cohort sample sizes are stored
--- @cohort: name of cohort table to use for feature extraction
--- @cohort_database_schema: schema where cohort table is stored
--- @results_database_schema: schema where cohort table is stored
--- @cohort_overlap_table: name of table where cohort overlap statistics are to be stored

drop table if exists @results_database_schema.@cohort_overlap_table;

insert into @results_database_schema.@cohort_overlap_table
select
  cohort_definition_id_1,
  cohort_definition_id_2,
  count(distinct subject_id)  / (n_persons_1 + n_persons_2 - count(distinct subject_id)) as jaccard_index,
  count(distinct subject_id) as n_overlap,
  sum(case when start_date_1 < start_date_2 then 1 else 0 end) as n_1_before_2,
  sum(case when start_date_1 > start_date_2 then 1 else 0 end) as n_2_before_1,
  sum(case when start_date_1 = start_date_2 then 1 else 0 end) as n_same_index
from (
	select
		ch1.cohort_definition_id as cohort_definition_id_1,
		cc1.num_persons as n_persons_1,
		ch2.cohort_definition_id as cohort_definition_id_2,
		cc2.num_persons as n_persons_2,
		ch1.subject_id,
		ch1.cohort_start_date as start_date_1,
		ch2.cohort_start_date as start_date_2
	from @cohort_database_schema.@cohort ch1
	inner join @cohort_database_schema.@cohort ch2
		on ch1.subject_id = ch2.subject_id and ch1.cohort_definition_id < ch2.cohort_definition_id
	inner join @results_database_schema.@cohort_counts cc1
		on ch1.cohort_definition_id = cc1.cohort_definition_id
	inner join @results_database_schema.@cohort_counts cc2
		on ch2.cohort_definition_id = cc2.cohort_definition_id)
group by cohort_definition_id_1, cohort_definition_id_2, n_persons_1, n_persons_2
order by cohort_definition_id_1, cohort_definition_id_2;
