library(testthat)
library(fcw.qaqc)

# Explore this error:
#   Error in map(.x, .f, ...) : ℹ In index: 1.
# Caused by error in `relocate()`:
#   ! Can't select columns that don't exist.
# ✖ Column `timestamp` doesn't exist.

test_that('api_puller.R works' , {
  # Add your test cases here
  expect_true(TRUE)  # Placeholder test
}) 
