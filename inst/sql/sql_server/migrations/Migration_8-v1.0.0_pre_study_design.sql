{DEFAULT @cse_cohort_person_time = cse_cohort_person_time}
{DEFAULT @cse_condition_concept_counts = cse_condition_concept_counts}

CREATE TABLE @database_schema.@table_prefix@cse_cohort_person_time (
    cohort_definition_id bigint,
    total_person_time_days bigint,
    database_id bigint,
    tar_id int,
    primary key (cohort_definition_id, tar_id, database_id)
);

CREATE TABLE @database_schema.@table_prefix@cse_condition_concept_counts (
    cohort_definition_id bigint,
    database_id bigint,
    condition_concept_id bigint,
    occurrence_count bigint,
    descendant_occurrence_count bigint,
    primary key (cohort_definition_id, condition_concept_id, database_id)
);