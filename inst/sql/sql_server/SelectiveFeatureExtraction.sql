-- PARAMETERS
--- @cohort_counts: name of table where cohort sample sizes are stored
--- @cohort: name of cohort table to use for feature extraction
--- @cdm_database_schema: CDM schema referenced by @results_database_schema
--- @results_database_schema: schema where cohort table is stored
--- @covariate_def_table: name of output table where covariate defs are stored
--- @covariate_means_table: name of output table where covariate means are stored

--- Create results tables
drop table if exists @results_database_schema.@covariate_def_table;
create table @results_database_schema.@covariate_def_table
(
  covariate_id bigint,
  covariate_name varchar(500),
  concept_id bigint,
  time_at_risk_start int,
  time_at_risk_end int,
  covariate_type varchar(255)  
);


drop table if exists @results_database_schema.@covariate_means_table;
create table @results_database_schema.@covariate_means_table
(
  cohort_definition_id bigint,
  covariate_id bigint,
  covariate_mean float
);

-- Calculate cohort counts and store
DROP TABLE IF EXISTS @results_database_schema.@cohort_counts;
create table @results_database_schema.@cohort_counts AS
	select count(distinct subject_id) as num_persons, sc1.cohort_definition_id
	from @cohort_database_schema.@cohort sc1
	group by sc1.cohort_definition_id;


--demographics: age group decile
drop table if exists #cov_summary;
create table #cov_summary as
select 
	scd1.cohort_definition_id,  
	100 + t1.covariate_id as covariate_id, 
	1.0*t1.num_persons/scd1.num_persons as covariate_mean
from @results_database_schema.@cohort_counts scd1
inner join 
(
    select 
    	cohort_definition_id, 
    	floor((year(sc1.cohort_start_date) - p1.year_of_birth)/10) as covariate_id,
    	count(sc1.subject_id) as num_persons
    from @cohort_database_schema.@cohort sc1
    inner join @cdm_database_schema.person p1
    on sc1.subject_id = p1.person_id
    where year(sc1.cohort_start_date) - p1.year_of_birth >= 0 and year(sc1.cohort_start_date) - p1.year_of_birth < 100   --data quality bounding
    group by cohort_definition_id, floor((year(sc1.cohort_start_date) - p1.year_of_birth)/10)
) t1
on scd1.cohort_definition_id = t1.cohort_definition_id
where 1.0*t1.num_persons/scd1.num_persons >= 0.01
;

insert into @results_database_schema.@covariate_means_table (cohort_definition_id, covariate_id, covariate_mean)
select cohort_definition_id, covariate_id, covariate_mean from #cov_summary;


insert into @results_database_schema.@covariate_def_table (covariate_id, covariate_name, covariate_type)
select distinct 
	covariate_id, 'Age decile: ' || cast(right(covariate_id, 2) as int)*10 || ' - ' || (cast(right(covariate_id, 2) as int)+1)*10-1 as covariate_name, 
	'Demographics' as covariate_type
from #cov_summary
;


--demographics: sex
drop table if exists #cov_summary;
create table #cov_summary as
select 
	scd1.cohort_definition_id,  
	t1.gender_concept_id as covariate_id, 
	1.0*t1.num_persons/scd1.num_persons as covariate_mean
from @results_database_schema.@cohort_counts scd1
inner join 
(
select 
	cohort_definition_id, 
	p1.gender_concept_id, 
	count(sc1.subject_id) as num_persons
from @cohort_database_schema.@cohort sc1
inner join @cdm_database_schema.person p1
on sc1.subject_id = p1.person_id
where gender_concept_id in (8532, 8507)
group by cohort_definition_id, p1.gender_concept_id
) t1
on scd1.cohort_definition_id = t1.cohort_definition_id
where 1.0*t1.num_persons/scd1.num_persons >= 0.01
;

insert into @results_database_schema.@covariate_means_table (cohort_definition_id, covariate_id, covariate_mean)
select cohort_definition_id, covariate_id, covariate_mean from #cov_summary;

insert into @results_database_schema.@covariate_def_table (covariate_id, covariate_name, covariate_type)
select 
	covariate_id, 'Sex: ' || c1.concept_name as covariate_name, 
	'Demographics' as covariate_type
from
(select distinct covariate_id from #cov_summary) cs1
inner join @cdm_database_schema.concept c1
on cs1.covariate_id = c1.concept_id
;

--conditions for presentation <=30d prior
drop table if exists #cov_summary;
create table #cov_summary as
select 
	scd1.cohort_definition_id, 
	t1.condition_concept_id as covariate_id, 
	1.0*t1.num_persons/scd1.num_persons as covariate_mean
from @results_database_schema.@cohort_counts scd1
inner join 
(
select 
	sc1.cohort_definition_id, 
	co1.condition_concept_id, 
	count(distinct sc1.subject_id) as num_persons
from @cohort_database_schema.@cohort sc1
inner join @cdm_database_schema.condition_occurrence co1
on sc1.subject_id = co1.person_id
and sc1.cohort_start_date >= dateadd(day,-30,co1.condition_start_date)
and sc1.cohort_start_date <= co1.condition_start_date
group by sc1.cohort_definition_id, co1.condition_concept_id
) t1
on scd1.cohort_definition_id = t1.cohort_definition_id
where 1.0*t1.num_persons/scd1.num_persons >= 0.01
and t1.condition_concept_id > 0
;

insert into @results_database_schema.@covariate_means_table (cohort_definition_id, covariate_id, covariate_mean)
select cohort_definition_id, covariate_id, covariate_mean from #cov_summary;


insert into @results_database_schema.@covariate_def_table (covariate_id, covariate_name, concept_id, time_at_risk_start, time_at_risk_end, covariate_type)
select 
	covariate_id, 
	'Condition in <=30d prior: ' || c1.concept_name as covariate_name, 
	covariate_id as concept_id, 
	-30 as time_at_risk_start, 
	0 as time_at_risk_end, 
	'Presentation' as covariate_type
from
(select distinct covariate_id from #cov_summary) cs1
inner join @cdm_database_schema.concept c1
on cs1.covariate_id = c1.concept_id
;

--conditions for medical history >30d prior
drop table if exists #cov_summary;
create table #cov_summary as
select 
	scd1.cohort_definition_id,  
	cast(t1.condition_concept_id as bigint)*1000 as covariate_id, 
	1.0*t1.num_persons/scd1.num_persons as covariate_mean
from @results_database_schema.@cohort_counts scd1
inner join 
(
select 
	sc1.cohort_definition_id, 
	co1.condition_concept_id, 
	count(distinct sc1.subject_id) as num_persons
from @cohort_database_schema.@cohort sc1
inner join @cdm_database_schema.condition_occurrence co1
on sc1.subject_id = co1.person_id
and datediff(day, co1.condition_start_date, sc1.cohort_start_date) > 30
group by sc1.cohort_definition_id, co1.condition_concept_id
) t1
on scd1.cohort_definition_id = t1.cohort_definition_id
where 1.0*t1.num_persons/scd1.num_persons >= 0.01
and t1.condition_concept_id > 0
;

insert into @results_database_schema.@covariate_means_table (cohort_definition_id, covariate_id, covariate_mean)
select cohort_definition_id, covariate_id, covariate_mean from #cov_summary;


insert into @results_database_schema.@covariate_def_table (covariate_id, covariate_name, concept_id, time_at_risk_start, time_at_risk_end, covariate_type)
select 
	covariate_id, 
	'Condition in >30d prior: ' || c1.concept_name as covariate_name, 
	covariate_id/1000 as concept_id, 
	-999 as time_at_risk_start, 
	-30 as time_at_risk_end, 
	'Medical history' as covariate_type
from
(select distinct covariate_id from #cov_summary) cs1
inner join @cdm_database_schema.concept c1
on cs1.covariate_id/1000 = c1.concept_id
;

--drug for conmed start>=30d, end>0d
-- comment was --drug for conmed start<=0d, end>0d
drop table if exists #cov_summary;
create table #cov_summary as
select scd1.cohort_definition_id,  cast(t1.drug_concept_id as bigint)*1000 as covariate_id, 1.0*t1.num_persons/scd1.num_persons as covariate_mean
from @results_database_schema.@cohort_counts scd1
inner join 
(
select sc1.cohort_definition_id, de1.drug_concept_id, count(distinct sc1.subject_id) as num_persons
from @cohort_database_schema.@cohort sc1
inner join @cdm_database_schema.drug_era de1
on sc1.subject_id = de1.person_id
and sc1.cohort_start_date >= dateadd(day,30,de1.drug_era_start_date)
and sc1.cohort_start_date >= dateadd(day,0,de1.drug_era_end_date)
group by sc1.cohort_definition_id, de1.drug_concept_id
) t1
on scd1.cohort_definition_id = t1.cohort_definition_id
where 1.0*t1.num_persons/scd1.num_persons >= 0.01
and t1.drug_concept_id > 0
;

select * from #cov_summary;


insert into @results_database_schema.@covariate_means_table (cohort_definition_id, covariate_id, covariate_mean)
select cohort_definition_id, covariate_id, covariate_mean from #cov_summary;


insert into @results_database_schema.@covariate_def_table (covariate_id, covariate_name, concept_id, time_at_risk_start, time_at_risk_end, covariate_type)
select covariate_id, 'Drug with start >30d prior: ' || c1.concept_name as covariate_name, covariate_id/1000 as concept_id, 0 as time_at_risk_start, 30 as time_at_risk_end, 'Prior medications' as covariate_type
from
(select distinct covariate_id from #cov_summary) cs1
inner join @cdm_database_schema.concept c1
on cs1.covariate_id/1000 = c1.concept_id
;

--visit context: all visits, by visit_concept_id, in interval [-30, 0]
drop table if exists #cov_summary;
create table #cov_summary as
select 
	scd1.cohort_definition_id, 
	t1.visit_concept_id as covariate_id, 
	1.0*t1.num_persons/scd1.num_persons as covariate_mean
from @results_database_schema.@cohort_counts scd1
inner join 
(
select 
	sc1.cohort_definition_id, 
	vo1.visit_concept_id, 
	count(distinct sc1.subject_id) as num_persons
from @cohort_database_schema.@cohort sc1
inner join @cdm_database_schema.visit_occurrence vo1
on sc1.subject_id = vo1.person_id
and sc1.cohort_start_date >= dateadd(day,-30,vo1.visit_start_date)
and sc1.cohort_start_date <= vo1.visit_start_date
group by sc1.cohort_definition_id, vo1.visit_concept_id
) t1
on scd1.cohort_definition_id = t1.cohort_definition_id
where 1.0*t1.num_persons/scd1.num_persons >= 0.01
and t1.visit_concept_id > 0
;

insert into @results_database_schema.@covariate_means_table (cohort_definition_id, covariate_id, covariate_mean)
select cohort_definition_id, covariate_id, covariate_mean from #cov_summary;


insert into @results_database_schema.@covariate_def_table (covariate_id, covariate_name, concept_id, time_at_risk_start, time_at_risk_end, covariate_type)
select 
	covariate_id, 
	'Visit in <=30d prior: ' || c1.concept_name as covariate_name, 
	covariate_id as concept_id, 
	-30 as time_at_risk_start, 
	0 as time_at_risk_end, 
	'Visit context' as covariate_type
from
(select distinct covariate_id from #cov_summary) cs1
inner join @cdm_database_schema.concept c1
on cs1.covariate_id = c1.concept_id;