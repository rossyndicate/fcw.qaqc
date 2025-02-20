#' @title Generate summary statistics for a given site parameter data frame.
#' @description  A function that generates summary statistics for a given site parameter data frame. The generated statistics include:
#'   - The next observation and previous observation values.
#'   - The rolling 7-point median of the value.
#'   - The rolling 7-point mean of the value.
#'   - The rolling 7-point standard deviation of the values.
#'   - The slope in relation to the observations ahead and behind.
#'   - The rolling 7-point slope of the observations.
#'   - The month and year of each data point.
#'   - The year-month combination.
#' @param site_param_df A single site-parameter data frame with a `mean` column
#' @return A data frame with summary statistics for a given site parameter data frame.
#' @examples
#' generate_summary_statistics(site_param_df = all_data_flagged$`archery-Actual Conductivity`)
#' generate_summary_statistics(site_param_df = all_data_flagged$`boxelder-Temperature`)

generate_summary_statistics <- function(site_param_df) {

  # Function to add a column if it doesn't already exist
  add_column_if_not_exists <- function(df, column_name, default_value = NA) {
    if (!column_name %in% colnames(df)) {
      df <- df %>% dplyr::mutate(!!sym(column_name) := default_value)
    }
    return(df)
  }

  summary_stats_df <- site_param_df %>%
    # To make appending of data possible, preserve historical data it it exists. Only
    # make these columns if they don't already exist
    add_column_if_not_exists(column_name = "front1") %>%
    add_column_if_not_exists(column_name = "back1") %>%
    add_column_if_not_exists(column_name = "rollmed") %>%
    add_column_if_not_exists(column_name = "rollavg") %>%
    add_column_if_not_exists(column_name = "rollsd") %>%
    add_column_if_not_exists(column_name = "slope_ahead") %>%
    add_column_if_not_exists(column_name = "slope_behind") %>%
    add_column_if_not_exists(column_name = "rollslope") %>%
    dplyr::mutate(
      front1 = ifelse(is.na(front1), dplyr::lead(mean, n = 1), front1),
      back1 = ifelse(is.na(back1), dplyr::lag(mean, n = 1), back1),
      # Add the median for a point and 6 points behind it:
      rollmed = ifelse(is.na(rollmed), RcppRoll::roll_median(mean, n = 7, align = 'right', na.rm = F, fill = NA_real_), rollmed),
      # Add the mean for a point and 6 points behind it:
      rollavg = ifelse(is.na(rollavg), RcppRoll::roll_mean(mean, n = 7, align = 'right', na.rm = F, fill = NA_real_), rollavg),
      # Add the standard deviation for a point and 6 points behind it:
      rollsd = ifelse(is.na(rollsd), RcppRoll::roll_sd(mean, n = 7, align = 'right', na.rm = F, fill = NA_real_), rollsd),
      # Determine the slope of a point in relation to the point ahead and behind.
      slope_ahead = ifelse(is.na(slope_ahead), (front1 - mean)/15, slope_ahead),
      slope_behind = ifelse(is.na(slope_behind), (mean - back1)/15, slope_behind),
      # Get the rolling 7-point slope (using itself + data of the past).
      rollslope = ifelse(is.na(rollslope), RcppRoll::roll_mean(slope_behind, n = 7, align = 'right', na.rm = F, fill = NA_real_), rollslope),
      # add some summary info for future us
      month = lubridate::month(DT_round),
      year = lubridate::year(DT_round),
      y_m = paste(year, '-', month),
      season = dplyr::case_when(month %in% c(12, 1, 2, 3, 4) ~ "winter_baseflow",
                                month %in% c(5, 6) ~ "snowmelt",
                                month %in% c(7, 8, 9) ~ "monsoon",
                                month %in% c(10, 11) ~ "fall_baseflow",
                                TRUE ~ NA)
    )

  return(summary_stats_df)

}
