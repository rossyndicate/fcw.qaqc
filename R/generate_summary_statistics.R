#' @title Calculate contextual statistics for water quality time series
#'
#' @description
#' Enhances water quality time series data with a comprehensive set of statistical
#' context metrics that support anomaly detection and pattern analysis. This function
#' calculates both temporal relationships (how each reading relates to adjacent points)
#' and rolling statistics (how each reading fits within recent trends).
#'
#' The function generates several types of contextual information:
#' - Adjacent value comparison: Values immediately before and after each point
#' - Rolling statistics: 7-point rolling median, mean, and standard deviation
#' - Rate-of-change metrics: Slope calculations for trend analysis
#' - Temporal context: Month, year, and seasonal classification
#'
#' These statistics provide the foundation for subsequent quality control processes
#' by establishing the expected behavior patterns and natural variability of each
#' parameter at each site. The 7-point window (covering ~90 minutes for 15-minute
#' data) captures short-term patterns while accommodating brief anomalies.
#'
#' @param site_param_df A dataframe containing time series data for a single
#' site-parameter combination. Must include:
#' - DT_round: Timestamp for each observation
#' - mean: The measured parameter value
#'
#' @return A dataframe containing all original columns plus the following added
#' statistical context columns:
#' - front1/back1: Values immediately ahead of and behind each point
#' - rollmed/rollavg/rollsd: 7-point rolling median, mean, and standard deviation
#' - slope_ahead/slope_behind: Rate of change to adjacent points
#' - rollslope: 7-point rolling slope average
#' - month/year/y_m: Temporal classification components
#' - season: Hydrological season classification (winter_baseflow, snowmelt,
#'   monsoon, fall_baseflow)
#'
#' @examples
#' # Calculate summary statistics for conductivity data at Archery site
#' archery_cond_stats <- generate_summary_statistics(
#'   site_param_df = combined_data$`archery-Specific Conductivity`
#' )
#'
#' # Calculate summary statistics for temperature data at Boxelder site
#' boxelder_temp_stats <- generate_summary_statistics(
#'   site_param_df = combined_data$`boxelder-Temperature`
#' )
#'
#' @seealso [combine_datasets()]
#' @seealso [add_field_notes()]

generate_summary_statistics <- function(site_param_df) {

  # Helper function to safely add a new column if it doesn't already exist
  # This preserves historical data when present and adds placeholder columns when needed
  add_column_if_not_exists <- function(df, column_name, default_value = NA) {
    if (!column_name %in% colnames(df)) {
      df <- df %>% dplyr::mutate(!!sym(column_name) := default_value)
    }
    return(df)
  }
  
  summary_stats_df <- site_param_df %>%
    # Initialize or preserve columns for statistical context
    # This approach maintains continuity with historical data when it exists
    add_column_if_not_exists(column_name = "front1") %>%
    add_column_if_not_exists(column_name = "back1") %>%
    add_column_if_not_exists(column_name = "rollmed") %>%
    add_column_if_not_exists(column_name = "rollavg") %>%
    add_column_if_not_exists(column_name = "rollsd") %>%
    add_column_if_not_exists(column_name = "slope_ahead") %>%
    add_column_if_not_exists(column_name = "slope_behind") %>%
    add_column_if_not_exists(column_name = "rollslope") %>%
    
    # Calculate temporal and statistical context metrics
    dplyr::mutate(
      # Adjacent values: Measurements immediately before and after this point
      # These help identify sudden changes and isolated anomalies
      front1 = ifelse(is.na(front1), dplyr::lead(mean, n = 1), front1),
      back1 = ifelse(is.na(back1), dplyr::lag(mean, n = 1), back1),
      
      # Rolling statistics with 7-point windows (this point + 6 preceding points)
      # These establish the recent behavioral pattern for each parameter
      
      # Rolling median: Robust measure of central tendency less affected by outliers
      rollmed = ifelse(is.na(rollmed), 
                     RcppRoll::roll_median(mean, n = 7, align = 'right', 
                                         na.rm = F, fill = NA_real_), 
                     rollmed),
      
      # Rolling mean: Average value over the recent window
      rollavg = ifelse(is.na(rollavg), 
                     RcppRoll::roll_mean(mean, n = 7, align = 'right', 
                                       na.rm = F, fill = NA_real_), 
                     rollavg),
      
      # Rolling standard deviation: Measure of recent variability
      rollsd = ifelse(is.na(rollsd), 
                    RcppRoll::roll_sd(mean, n = 7, align = 'right', 
                                    na.rm = F, fill = NA_real_), 
                    rollsd),
      
      # Rate-of-change metrics for trend analysis
      # For 15-minute data, dividing by 15 converts to units per minute
      
      # Slope to the next point (looking forward)
      slope_ahead = ifelse(is.na(slope_ahead), (front1 - mean)/15, slope_ahead),
      
      # Slope from the previous point (looking backward)
      slope_behind = ifelse(is.na(slope_behind), (mean - back1)/15, slope_behind),
      
      # Rolling average slope over the recent window
      rollslope = ifelse(is.na(rollslope), 
                       RcppRoll::roll_mean(slope_behind, n = 7, align = 'right', 
                                         na.rm = F, fill = NA_real_), 
                       rollslope),
      
      # Temporal context for seasonal pattern analysis
      month = lubridate::month(DT_round),
      year = lubridate::year(DT_round),
      y_m = paste(year, '-', month),
      
      # Hydrological season classification based on month
      # This reflects the distinct flow regimes of the watershed
      season = dplyr::case_when(
        month %in% c(12, 1, 2, 3, 4) ~ "winter_baseflow",  # Winter base flow period
        month %in% c(5, 6) ~ "snowmelt",                   # Spring snowmelt period
        month %in% c(7, 8, 9) ~ "monsoon",                 # Summer monsoon period
        month %in% c(10, 11) ~ "fall_baseflow",            # Fall base flow period
        TRUE ~ NA)
    )
  
  return(summary_stats_df)
}