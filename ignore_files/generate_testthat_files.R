# This function generates the testthat files in the testthat folder

library(here)
library(tidyverse)
library(usethis)
library(devtools)
devtools::load_all()

r_file_paths <- basename(list.files(here("R"), recursive = TRUE, full.names = F))

walk(r_file_paths, function(file_name){
  
  # Create the testthat file
  use_test(file_name, open = FALSE)
  
  # Alter the test file
  con <- file(here("tests", "testthat", paste0("test-", file_name)), "w")
  writeLines(c("library(testthat)", 
               "library(fcw.qaqc)",
               "set.seed(123)",
               "# Read in test data for fuc",
               paste0("test_that('", file_name," works' , {"),
               "  # Add your test cases here",
               "  expect_true(TRUE)  # Placeholder test",
               "})"), con)
  close(con)
  
})
