# library(tidyverse)
# library(testthat)
# library(fcw.qaqc)
# 
# # Test when both inputs are empty
# test_that("`final_data_binder()` errors out when both data sources are empty.", 
#           {
#             new_data <- NULL
#             historical_data <- NULL
#             expect_error(final_data_binder(
#               new_flagged_data = new_data,
#               historical_flagged_data = historical_data
#             ), "No data provided to `final_data_binder()`.")
#           })
# 
# # Test when historical data is present, but new incoming data is empty
# test_that("`final_data_binder()` returns historical data when incoming data is empty.", 
#           {
#             new_data <- NULL
#             historical_data <- list("site-Parameter" = tibble(test1 = c(1,2,3)))
#             expect_equal(final_data_binder(
#               new_flagged_data = new_data,
#               historical_flagged_data = historical_data
#             ), list("site-Parameter" = tibble(test1 = c(1,2,3))))
#           })
# 
# # Test when new incoming data is present, but historical data is empty
# test_that("`final_data_binder()` returns incoming data when historical data is empty.", 
#           {
#             new_data <- list("site-Parameter" = tibble(test2 = c(1,2,3)))
#             historical_data <- NULL
#             expect_equal(final_data_binder(
#               new_flagged_data = new_data,
#               historical_flagged_data = historical_data
#             ), list("site-Parameter" = tibble(test2 = c(1,2,3))))
#           })
# 
# # Test when both data inputs are present
# 
# test_that('final_data_binder.R works' , {
#   
#   new_data <- read_rds(test_path("data", "test_finalflags_data.RDS"))
#   new_data <- new_data[grep("udall-Chl-a Fluorescence", names(new_data))]
#   
#   historical_data <- read_rds(test_path("data", "test_historical_data.RDS")) %>% 
#     mutate(auto_flag = as.character(auto_flag)) %>% 
#     split(f = list(.$site, .$parameter), sep = "-") %>%
#     purrr::keep(~!is.null(.) & nrow(.) > 0)
#     
#   historical_data <- historical_data[grep("udall-Chl-a Fluorescence", names(historical_data))]
#   
#   # Add your test cases here
#   expect_true(TRUE)  # Placeholder test
# })
# 
# # does final data binder save new data?