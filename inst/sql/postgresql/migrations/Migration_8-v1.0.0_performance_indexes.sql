-- Performance indexes for ComparatorSelectionExplorer
-- Migration 8 - v1.1.0
-- Addresses slow cohort search and similarity score queries on PostgreSQL.
{DEFAULT @table_prefix = ''}

-- ---------------------------------------------------------------------------
-- 1. cse_cosine_similarity_score
--    The main similarity query filters by
--      WHERE cohort_definition_id_1 = X OR cohort_definition_id_2 = X
--    The existing PK covers lookups by cohort_definition_id_1 (leftmost key),
--    but not by cohort_definition_id_2. Adding an index on the reverse order
--    makes the OR branch equally fast. The index is created on the parent
--    partitioned table; PostgreSQL will propagate it to all child partitions.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_cse_sim_score_id2_db
  ON @database_schema.@table_prefixcse_cosine_similarity_score (cohort_definition_id_2, database_id);

-- Also index covariate_type to support the WHERE NOT IN ('average','Co-occurrence') filter.
CREATE INDEX IF NOT EXISTS idx_cse_sim_score_cov_type
  ON @database_schema.@table_prefixcse_cosine_similarity_score (covariate_type);

-- ---------------------------------------------------------------------------
-- 2. cg_cohort_definition - cohort name search
--    getCohortDefinitions fetches ALL rows; the API then filters in R with grepl().
--    Enable trigram index so the search can be pushed into SQL (see query note
--    below). pg_trgm must be installed once per database by a superuser:
--      CREATE EXTENSION IF NOT EXISTS pg_trgm;
--    The GIN index then supports ILIKE / % LIKE % in O(log n) instead of O(n).
-- ---------------------------------------------------------------------------
--CREATE EXTENSION IF NOT EXISTS pg_trgm;

--CREATE INDEX IF NOT EXISTS idx_cg_cohort_def_name_trgm
  --ON @database_schema.@table_prefixcg_cohort_definition
  --USING GIN (cohort_name gin_trgm_ops);

-- Plain btree for ORDER BY cohort_name (used in every cohort list query)
CREATE INDEX IF NOT EXISTS idx_cg_cohort_def_name_btree
  ON @database_schema.@table_prefixcg_cohort_definition (cohort_name);

-- ---------------------------------------------------------------------------
-- 3. cse_cohort_tag - tag lookup
--    getCohortsByTag and getCohortTags filter/join on tag and cohort_definition_id.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_cse_cohort_tag_tag
  ON @database_schema.@table_prefixcse_cohort_tag (tag, cohort_definition_id);

-- ---------------------------------------------------------------------------
-- 4. cse_cohort_count - join key used in every similarity query
--    PK is (cohort_definition_id, database_id). An additional index covering
--    the reverse order helps when the planner joins from the database_id side.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_cse_cohort_count_db
  ON @database_schema.@table_prefixcse_cohort_count (database_id, cohort_definition_id);

-- ---------------------------------------------------------------------------
-- 5. cse_atc_level - expression join
--    The similarity query joins on:
--      t.cohort_definition_id_1 / 1000 = atc.drug_concept_id_1  (and symmetric)
--    Division is not indexable. Pre-computed expression indexes allow the planner
--    to use an index scan instead of a seq-scan + compute for this join.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_cse_atc_level_id1
  ON @database_schema.@table_prefixcse_atc_level (drug_concept_id_1, drug_concept_id_2);

CREATE INDEX IF NOT EXISTS idx_cse_atc_level_id2
  ON @database_schema.@table_prefixcse_atc_level (drug_concept_id_2, drug_concept_id_1);
