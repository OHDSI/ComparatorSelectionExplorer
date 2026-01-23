test_that("validateCohortTags works correctly", {
  # Valid tags
  validTags <- list("exposure" = c(1, 2, 3), "outcome" = c(4, 5))
  expect_true(validateCohortTags(validTags))
  
  # NULL is valid
  expect_true(validateCohortTags(NULL))
  
  # Invalid: not a list
  expect_error(validateCohortTags(c(1, 2, 3)), "must be a named list")
  
  # Invalid: unnamed list
  expect_error(validateCohortTags(list(c(1, 2), c(3, 4))), "must be a named list")
  
  # Invalid: non-numeric values
  expect_error(validateCohortTags(list("tag1" = c("a", "b"))), "must be numeric")
})

test_that("flattenCohortTags extracts unique IDs", {
  tags <- list("exposure" = c(1, 2, 3), "outcome" = c(3, 4, 5))
  flattened <- flattenCohortTags(tags)
  
  expect_equal(sort(flattened), c(1, 2, 3, 4, 5))
  expect_type(flattened, "integer")
  
  # NULL returns NULL
  expect_null(flattenCohortTags(NULL))
})

test_that("cohortTagsToDataFrame creates correct structure", {
  tags <- list(
    "exposure" = c(1, 2),
    "outcome" = c(2, 3),
    "indication" = c(1, 3)
  )
  
  df <- cohortTagsToDataFrame(tags)
  
  expect_s3_class(df, "data.frame")
  expect_equal(ncol(df), 2)
  expect_equal(names(df), c("cohortDefinitionId", "tag"))
  expect_equal(nrow(df), 6)  # 2 + 2 + 2 = 6 rows
  
  # Check specific mappings
  expect_true(all(df$cohortDefinitionId[df$tag == "exposure"] %in% c(1, 2)))
  expect_true(all(df$cohortDefinitionId[df$tag == "outcome"] %in% c(2, 3)))
  expect_true(all(df$cohortDefinitionId[df$tag == "indication"] %in% c(1, 3)))
  
  # Cohort 1 should have tags: exposure, indication
  cohort1Tags <- df$tag[df$cohortDefinitionId == 1]
  expect_equal(sort(cohort1Tags), c("exposure", "indication"))
  
  # Cohort 2 should have tags: exposure, outcome
  cohort2Tags <- df$tag[df$cohortDefinitionId == 2]
  expect_equal(sort(cohort2Tags), c("exposure", "outcome"))
  
  # Cohort 3 should have tags: indication, outcome
  cohort3Tags <- df$tag[df$cohortDefinitionId == 3]
  expect_equal(sort(cohort3Tags), c("indication", "outcome"))
})

test_that("cohortTagsToDataFrame handles NULL correctly", {
  df <- cohortTagsToDataFrame(NULL)
  
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 0)
  expect_equal(names(df), c("cohortDefinitionId", "tag"))
})

test_that("convertTargetCohortIdsToTags provides backward compatibility", {
  oldFormat <- c(1, 2, 3, 4, 5)
  converted <- convertTargetCohortIdsToTags(oldFormat)
  
  expect_type(converted, "list")
  expect_equal(names(converted), "default")
  expect_equal(converted$default, as.integer(oldFormat))
  
  # NULL returns NULL
  expect_null(convertTargetCohortIdsToTags(NULL))
})

test_that("cohortTagsToDataFrame maintains cohort-tag relationships", {
  # Test that a cohort can have multiple tags
  tags <- list(
    "tag1" = c(1, 2, 3),
    "tag2" = c(2, 4),
    "tag3" = c(1, 2, 4)
  )
  
  df <- cohortTagsToDataFrame(tags)
  
  # Cohort 1 should appear in tag1 and tag3
  cohort1Rows <- df[df$cohortDefinitionId == 1, ]
  expect_equal(nrow(cohort1Rows), 2)
  expect_setequal(cohort1Rows$tag, c("tag1", "tag3"))
  
  # Cohort 2 should appear in all three tags
  cohort2Rows <- df[df$cohortDefinitionId == 2, ]
  expect_equal(nrow(cohort2Rows), 3)
  expect_setequal(cohort2Rows$tag, c("tag1", "tag2", "tag3"))
  
  # Cohort 4 should appear in tag2 and tag3
  cohort4Rows <- df[df$cohortDefinitionId == 4, ]
  expect_equal(nrow(cohort4Rows), 2)
  expect_setequal(cohort4Rows$tag, c("tag2", "tag3"))
})
