{DEFAULT @table_prefix = ''}
{DEFAULT @atc_level = atc_level}

DROP TABLE IF EXISTS @database_schema.@table_prefix@atc_level;
CREATE TABLE @database_schema.@table_prefix@atc_level (
    cohort_definition_id_1 bigint,
	cohort_definition_id_2 bigint,
	level_closest_atc_relation int,
	level_furthest_atc_relation int,
	atc_1_related int,
	atc_2_related int,
	atc_3_related int,
	atc_4_related int,
	atc_5_related int,
    PRIMARY KEY (cohort_definition_id_1, cohort_definition_id_2)
);