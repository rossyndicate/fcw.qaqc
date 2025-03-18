#' @title Flag suspect data based on context patterns
#'
#' @description
#' Identifies and flags potentially problematic measurements that appear valid in isolation 
#' but are suspect due to surrounding data patterns. The function applies the "suspect data" 
#' flag in two scenarios:
#' 
#' 1. When an unflagged measurement falls within a 2-hour window where ≥50% of surrounding 
#'    data points have quality flags
#' 
#' 2. When an isolated measurement appears within a 2-hour window where ≥90% of surrounding 
#'    data is missing
#'
#' @param df A dataframe containing water quality measurements. Must include columns:
#' - `mean`: The measurement value (NA indicates missing data)
#' - `auto_flag`: Existing quality flags
#'
#' @return A dataframe with the same structure as the input, but with the `auto_flag` 
#' column updated to include "suspect data" flags for measurements that match the 
#' specified conditions.
#'
#' @examples
#' # Flag suspect data in conductivity measurements
#' archery_conductivity_flagged <- add_suspect_flag(df = final_flags$`archery-Actual Conductivity`)
#'
#' # Flag suspect data in temperature measurements
#' boxelder_temp_flagged <- add_suspect_flag(df = final_flags$`boxelder-Temperature`)

add_suspect_flag <- function(df) {
  # Define flags that should be excluded from the suspect data analysis
  auto_flag_string <- "sonde not employed|missing data|site visit|sv window|reported sonde burial|reported sensor biofouling|reported depth calibration malfunction|reported sensor malfunction"
  
  # Function to check if ≥50% of points in a window have flags
  check_2_hour_window_fail <- function(x) {
    sum(x) / length(x) >= 0.5
  }
  
  # First pass: Flag isolated good data points within mostly flagged regions
  df_test <- df %>%
    # Create binary indicator for relevant flags (1 = flagged, 0 = not flagged or excluded flag type)
    dplyr::mutate(auto_flag_binary = ifelse(is.na(auto_flag) | grepl(auto_flag_string, auto_flag) | auto_flag == "suspect data", 0, 1)) %>%
    # Check if ≥50% of data in 2-hour windows (8 observations) has flags
    dplyr::mutate(over_50_percent_fail_window_right = zoo::rollapply(auto_flag_binary, width = 8, FUN = check_2_hour_window_fail, fill = NA, align = "right")) %>%
    dplyr::mutate(over_50_percent_fail_window_center = zoo::rollapply(auto_flag_binary, width = 8, FUN = check_2_hour_window_fail, fill = NA, align = "center")) %>%
    # Flag unflagged observations within heavily flagged windows as "suspect data"
    dplyr::mutate(auto_flag = as.character(ifelse(is.na(auto_flag) &
                                                    (over_50_percent_fail_window_right == TRUE | over_50_percent_fail_window_center == TRUE),
                                                  "suspect data", auto_flag)))
  
  # Function to check if ≥90% of points in a window are missing
  check_2_hour_window_missing <- function(x) {
    sum(x) / length(x) >= (8/9)
  }
  
  # Second pass: Flag isolated measurements within mostly missing data regions
  df_test <- df_test %>%
    # Create binary indicator for missing data (1 = missing, 0 = present)
    dplyr::mutate(auto_flag_binary = ifelse(is.na(mean), 1, 0)) %>%
    # Check if ≥90% of data in 2-hour windows is missing
    dplyr::mutate(over_90_percent_missing_window_right = zoo::rollapply(auto_flag_binary, width = 8, FUN = check_2_hour_window_missing, fill = NA, align = "right")) %>%
    dplyr::mutate(over_90_percent_missing_window_center = zoo::rollapply(auto_flag_binary, width = 8, FUN = check_2_hour_window_missing, fill = NA, align = "center")) %>%
    # Flag valid but isolated measurements as "suspect data"
    dplyr::mutate(auto_flag = as.character(ifelse(is.na(auto_flag) & !is.na(mean) &
                                                    (over_90_percent_missing_window_right == TRUE & over_90_percent_missing_window_center == TRUE),
                                                  "suspect data", auto_flag)))
  
  return(df_test)
}