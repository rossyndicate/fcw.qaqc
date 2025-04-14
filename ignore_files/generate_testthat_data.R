# testthat data directory
testthat_data_dir <- function(file_name){
  return(here("tests", "testthat", "data", file_name))
}

# historical data
test_historical_data <- read_rds(here("data", "pwqn_data.RDS"))
write_rds(test_historical_data, testthat_data_dir("test_historical_data.RDS"))

# mWater data pulled from API, but not yet gone through `grab_mWater_sensor_notes()` or `grab_mWater_malfunction_notes`
write_rds(mWater_data, testthat_data_dir("test_mWater_data.RDS"))

# Initial HydroVu API data pull
walk(list.files(here("data", "api"), full.names = TRUE),
     function(file_path){
       file.copy(
         from = file_path,
         to = testthat_data_dir(paste0("api/test_",basename(file_path)))
       )
     })

# Munged data from the API pull
write_rds(new_data, testthat_data_dir("test_munged_data.RDS"))

# Tidied data from the munged data
write_rds(new_data_tidied_list, testthat_data_dir("test_tidied_data.RDS"))

# Combined tidied data and field note data
write_rds(combined_data, testthat_data_dir("test_combined_data.RDS"))

# Data with summary statistics
write_rds(all_data_summary_stats_list, testthat_data_dir("test_summarystats_data.RDS"))

# Single sensor flags
write_rds(single_sensor_flags, testthat_data_dir("test_singlesensorflags_data.RDS"))

# Multiple sensors in a single sonde flags
write_rds(intersensor_flags, testthat_data_dir("test_intersensorflags_data.RDS"))

# Final sensor flags
write_rds(final_flags, testthat_data_dir("test_finalflags_data.RDS"))

# Final Historical Bind
write_rds(final_historical_bind, testthat_data_dir("test_finalhistoricalbind_data.RDS"))
