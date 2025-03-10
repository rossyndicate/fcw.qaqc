#' @title Add flags related to calculated parameter seasonal ranges.
#' @description A function that checks 2 different and separate conditions related to the
#' calculated parameter seasonal ranges. The 'outside of seasonal range' flag is
#' added if the `mean` value is outside the seasonal 1st - 99th percentile. The
#' 'slope violation' flag is added if the `slope_ahead` or `slope_behind` value
#' is greater than or equal to the `t_slope_behind_99` value, or if the
#' `slope_ahead` or `slope_behind` value is less than or equal to the
#' `t_slope_behind_01` value. The `t_slope_behind_99` and `t_slope_behind_01`
#' values are specific to each site-parameter.
#' @param df A data frame with a `flag` column.
#' @param threshold_table A dataframe containing seasonal threshold values.
#' @return A data frame with a `flag` column that has been updated with the
#' relevant calculated seasonal range flags.

add_seasonal_flag <- function(df, threshold_table) {

  # get the site name from the site column in the df of interest
  site_name <- unique(na.omit(df$site))
  # get the parameter from the parameter column in the df of interest
  parameter_name <- unique(na.omit(df$parameter))

  lookup <- threshold_table %>%
    filter(site == site_name & parameter == parameter_name) %>%
    select(!c(site, parameter))

  df <- df %>%
    # Using seasonal cut-offs...
    left_join(lookup, by = "season") %>%
    # ... flag obs that are outside the seasonal 1st - 99th percentile range:
    add_flag((mean < t_mean01 | mean > t_mean99), "outside of seasonal range") %>%
    # flag obs whose slope is outside the 1st - 99th percentile range:
    add_flag(((slope_ahead >= t_slope_behind_99 | slope_behind >= t_slope_behind_99) |
              (slope_ahead <= t_slope_behind_01 | slope_behind <= t_slope_behind_01)), "slope violation")

  return(df)

}
