#' @title Flag data outside seasonal ranges and with abnormal slopes
#'
#' @description
#' Identifies and flags water quality measurements that fall outside expected seasonal 
#' patterns based on historical data. This function applies two distinct quality flags:
#'
#' 1. "outside of seasonal range" - Applied when a measurement falls outside the 1st-99th
#' percentile range of historical measurements for that site, parameter, and season
#'
#' 2. "slope violation" - Applied when the rate of change (slope) between consecutive
#' measurements exceeds historical thresholds, potentially indicating sensor malfunction
#' or unusual environmental events
#'
#' These flags help distinguish between natural seasonal variation and potentially
#' problematic measurements requiring further investigation.
#'
#' @param df A data frame containing water quality measurements. Must include columns:
#' - `site`: Standardized site name
#' - `parameter`: The measurement type
#' - `mean`: The calculated mean value of measurements
#' - `season`: Season categorization for the measurement
#' - `slope_ahead`: Rate of change to the next measurement
#' - `slope_behind`: Rate of change from the previous measurement
#' - `flag`: Existing quality flags (will be updated by this function)
#'
#' @param threshold_table A dataframe containing seasonal threshold values with columns:
#' - `site`: Site name matching the df site column
#' - `parameter`: Parameter name matching the df parameter column
#' - `season`: Season categorization (e.g., "spring", "summer")
#' - `t_mean01`: 1st percentile threshold for the mean value
#' - `t_mean99`: 99th percentile threshold for the mean value
#' - `t_slope_behind_01`: 1st percentile threshold for slope values
#' - `t_slope_behind_99`: 99th percentile threshold for slope values
#'
#' @return A data frame with the same structure as the input, but with the flag
#' column updated to include "outside of seasonal range" and/or "slope violation"
#' flags as appropriate.
#'
#' @examples
#' # Flag temperature measurements with seasonal violations
#' riverbluffs_temp_flagged <- add_seasonal_flag(
#'   df = all_data$`riverbluffs-Temperature`,
#'   threshold_table = seasonal_thresholds
#' )
#'
#' # Flag dissolved oxygen measurements with seasonal violations
#' boxelder_do_flagged <- add_seasonal_flag(
#'   df = all_data$`boxelder-DO`,
#'   threshold_table = seasonal_thresholds
#' )
#'
#' @seealso [add_spec_flag()]
#' @seealso [add_flag()]

add_seasonal_flag <- function(df, threshold_table) {
  # Extract the unique site name from the dataframe
  # This assumes a single site per dataframe
  site_name <- unique(na.omit(df$site))
  
  # Extract the unique parameter name from the dataframe
  # This assumes a single parameter type per dataframe
  parameter_name <- unique(na.omit(df$parameter))
  
  # Filter threshold table to get only values relevant to this site-parameter combination
  # and remove the site and parameter columns as they're no longer needed for joining
  lookup <- threshold_table %>%
    filter(site == site_name & parameter == parameter_name) %>%
    select(!c(site, parameter))
  
  df <- df %>%
    # Join seasonal thresholds to the data using the season column as the key
    left_join(lookup, by = "season") %>%
    
    # Flag observations outside the seasonal 1st-99th percentile range
    # These may represent unusual environmental conditions or sensor issues
    add_flag((mean < t_mean01 | mean > t_mean99), "outside of seasonal range") %>%
    
    # Flag observations with abnormal rates of change (slopes)
    # Unusually rapid changes may indicate sensor malfunction or data artifacts
    add_flag(((slope_ahead >= t_slope_behind_99 | slope_behind >= t_slope_behind_99) |
                (slope_ahead <= t_slope_behind_01 | slope_behind <= t_slope_behind_01)), "slope violation")
  
  return(df)
}