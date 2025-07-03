# Clean up global variable warnings
cat('utils::globalVariables(c(
  ".", "DO_drift_binary", "DT", "Depth_flag", "Location", "Parameter", 
  "Temperature_flag", "Units", "auto_flag_binary", "cal_report_collected", 
  "cals_performed", "center", "clean_path", "coalesce", "crew", "days",
  "do_flag", "end_DT", "end_dt", "failed", "field_season", "flag_binary",
  "flag_down", "flag_up", "historical", "historical_data", "hours",
  "hv_locations_all", "hv_token", "id", "intersensor_flags", "intersensored",
  "last_site_visit", "mal_flag", "malfunction_end_dt", "map", "map_dfr", "name",
  "notes", "over_50_percent_fail_window_center", "over_50_percent_fail_window_right",
  "over_90_percent_missing_window_center", "over_90_percent_missing_window_right",
  "overlapping_flag", "r2_l_center", "r2_l_right", "r2_s_center", "r2_s_right",
  "rdo_cap_condition", "rdo_cap_replaced", "relative_depth", "relocate", "right",
  "rollavg", "rollmed", "rollsd", "rollslope", "rowid", "sensor_deployed",
  "sensor_malfunction", "sensor_pulled", "sensor_swapped_notes", "sensors_cleaned",
  "site_down", "site_other", "site_up", "slope_ahead", "slope_behind", "sn_deployed",
  "sn_removed", "sonde_employed", "start_DT", "start_DT.x", "start_DT.y", "start_dt",
  "start_time_mst", "t_mean01", "t_mean99", "t_slope_behind_01", "t_slope_behind_99",
  "tibble", "tightest_r", "timestamp", "type", "upload_adls_file", "visit_comments",
  "visit_type", "visit_type_other", "walk", "which_sensor_malfunction", "wiper_working",
  "write_csv"
))', file="R/globals.R")

# Fix imports with imports.R
cat('
#\' @importFrom utils timestamp
#\' @importFrom readr write_csv
#\' @importFrom AzureStor upload_adls_file
#\' @importFrom purrr map map_dfr walk
#\' @importFrom tibble tibble
#\' @importFrom dplyr coalesce relocate
#\' @importFrom lubridate days hours
#\' @importFrom stringr str_detect str_extract
NULL
', file="R/imports.R")

# Now document and build
devtools::document()
devtools::build()
devtools::install()