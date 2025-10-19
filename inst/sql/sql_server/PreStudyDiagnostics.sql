

DROP TABLE IF EXISTS @results_database_schema.@condition_concept_counts;
CREATE TABLE @results_database_schema.@condition_concept_counts (
    cohort_definition_id bigint,
    condition_concept_id bigint,
    condition_concept_name varchar(255),
    occurrence_count bigint,
    descendant_occurrence_count bigint
);

INSERT INTO @results_database_schema.@condition_concept_counts
SELECT
    sc.cohort_definition_id,
    ca.ancestor_concept_id AS condition_concept_id,
    c.concept_name AS condition_concept_name,
    COUNT(DISTINCT CASE
        WHEN co.condition_concept_id = ca.ancestor_concept_id
        THEN co.person_id
    END) AS occurrence_count,
    COUNT(DISTINCT CASE
        WHEN co.condition_concept_id != ca.ancestor_concept_id
        THEN co.person_id
    END) AS descendant_occurrence_count
FROM @cohort_database_schema.@cohort sc
INNER JOIN @cdm_database_schema.condition_occurrence co
    ON sc.subject_id = co.person_id
    AND DATEDIFF(day, sc.cohort_start_date, co.condition_start_date) > 1
INNER JOIN @cdm_database_schema.concept_ancestor ca
    ON ca.descendant_concept_id = co.condition_concept_id
INNER JOIN @cdm_database_schema.concept c
    ON ca.ancestor_concept_id = c.concept_id
    AND c.domain_id = 'Condition'
    AND c.standard_concept = 'S'
    AND c.vocabulary_id = 'SNOMED'
GROUP BY sc.cohort_definition_id, ca.ancestor_concept_id, c.concept_name;

-- Drop table if it already exists
DROP TABLE IF EXISTS @results_database_schema.@cohort_person_time;

-- Create table for aggregated person-time per cohort
CREATE TABLE @results_database_schema.@cohort_person_time (
    cohort_definition_id bigint,
    total_person_time_days bigint,
    tar_id int
);

-- Insert aggregated person-time
INSERT INTO @results_database_schema.@cohort_person_time
SELECT
    sc.cohort_definition_id,
    SUM(
        DATEDIFF(
            day,
            de.drug_exposure_start_date,
            COALESCE(de.drug_exposure_end_date, de.drug_exposure_start_date)
        ) + 1
    ) AS total_person_time_days,
    1 AS tar_id
FROM @cohort_database_schema.@cohort sc
INNER JOIN @cdm_database_schema.drug_exposure de
    ON sc.subject_id = de.person_id
    AND de.drug_exposure_start_date >= sc.cohort_start_date
    AND (de.drug_exposure_end_date IS NULL OR de.drug_exposure_end_date <= sc.cohort_end_date)
GROUP BY sc.cohort_definition_id;