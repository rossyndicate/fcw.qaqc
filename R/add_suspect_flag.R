#' @title Add flags to the `flag` column of a dataframe based on large swaths of suspect data, or random values within a wide time period of missing data.
#' @description
#' "24hr anomaly flag" is added if more than 50% of the data points in a 24 hour window are flagged.
#' "anomaly window" flag is added if the point is included in a 24hr anomaly.
#' @param df A data frame with a `flag` column.
#' @return A data frame with a `flag` column that has been updated with the relevant calculated seasonal range flags.
#' @examples
#' add_range_flags(df = all_data_flagged$`archery-Actual Conductivity`)
#' add_range_flags(df = all_data_flagged$`boxelder-Temperature`)


add_suspect_flag <- function(df) {

  # these are the flags that we don't want to perform this exercise across
  auto_flag_string <- "sonde not employed|missing data|site visit|sv window|reported sonde burial|reported sensor biofouling|reported depth calibration malfunction|reported sensor malfunction"

  # Define a function to check if a given 2-hour window has >= 50% fails
  check_2_hour_window_fail <- function(x) {
    sum(x) / length(x) >= 0.5
  }

  df_test <- df %>%
    #add_column_if_not_exists(column_name = "auto_flag_binary", default_value = 0) %>%
    dplyr::mutate(auto_flag_binary = ifelse(is.na(auto_flag) | grepl(auto_flag_string, auto_flag) | auto_flag == "suspect data", 0, 1)) %>%
    dplyr::mutate(over_50_percent_fail_window_right = zoo::rollapply(auto_flag_binary, width = 8, FUN = check_2_hour_window_fail, fill = NA, align = "right")) %>%
    dplyr::mutate(over_50_percent_fail_window_center = zoo::rollapply(auto_flag_binary, width = 8, FUN = check_2_hour_window_fail, fill = NA, align = "center")) %>%
    #dplyr::mutate(over_50_percent_fail_window_left = zoo::rollapply(auto_flag_binary, width = 8, FUN = check_3_hour_window_fail, fill = NA, align = "left")) %>%
    dplyr::mutate(auto_flag = as.character(ifelse(is.na(auto_flag) &
                                             (over_50_percent_fail_window_right == TRUE | over_50_percent_fail_window_center == TRUE),
                                              "suspect data", auto_flag)))
  
  
  
  # Define a function to check if a given 2-hour window has >= 90% missing data
  check_2_hour_window_missing <- function(x) {
    sum(x) / length(x) >= 0.9
  }
  
  df_test <- df_test %>%
    dplyr::mutate(auto_flag_binary = ifelse(is.na(mean), 1, 0)) %>%
    dplyr::mutate(over_90_percent_missing_window_right = zoo::rollapply(auto_flag_binary, width = 8, FUN = check_2_hour_window_fail, fill = NA, align = "right")) %>%
    dplyr::mutate(over_90_percent_missing_window_center = zoo::rollapply(auto_flag_binary, width = 8, FUN = check_2_hour_window_fail, fill = NA, align = "center")) %>%
    dplyr::mutate(auto_flag = as.character(ifelse(is.na(auto_flag) & !is.na(mean) &
                                             (over_90_percent_missing_window_right == TRUE & over_90_percent_missing_window_center == TRUE),
                                           "suspect data", auto_flag)))
  
    return(df_test)
}


