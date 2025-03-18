#' @title Flag sensor drift in optical measurements
#'
#' @description
#' Identifies and flags periods when optical sensors show evidence of progressive drift,
#' which often indicates biofouling or calibration issues. The function analyzes time series
#' patterns to detect when measurements show a steady linear trend over time that is
#' unlikely to represent natural environmental conditions.
#'
#' The function only processes optical parameters prone to biofouling:
#' - FDOM Fluorescence
#' - Turbidity 
#' - Chl-a Fluorescence
#'
#' For other parameters, the function returns the dataframe unchanged.
#'
#' @param df A data frame containing water quality measurements. Must include columns:
#' - `parameter`: The measurement type (function checks if it's an optical parameter)
#' - `mean`: The calculated mean value of measurements
#' - `flag`: Existing quality flags (will be updated by this function)
#' - `DT_round`: Timestamp for rolling window analysis
#'
#' @return A data frame with the same structure as the input, but with the `flag`
#' column updated to include "drift" for periods showing evidence of sensor drift.
#'
#' @examples
#' # Flag drift in turbidity measurements
#' turbidity_flagged <- add_drift_flag(df = all_data_summary_stats_list$`riverbluffs-Turbidity`)
#'
#' # Flag drift in chlorophyll measurements
#' chlorophyll_flagged <- add_drift_flag(df = all_data_summary_stats_list$`boxelder-Chl-a Fluorescence`)
#'
#' # Function will not modify non-optical parameter data
#' temperature_data <- add_drift_flag(df = all_data_summary_stats_list$`riverbluffs-Temperature`)
#'
#' @seealso [add_flag()]

add_drift_flag <- function(df){
  # Only apply drift detection to optical parameters prone to biofouling
  if(unique(df$parameter) %in% c("FDOM Fluorescence", "Turbidity", "Chl-a Fluorescence")){
 
    # Define function to detect linear trends in rolling windows
    # Returns R-squared value of linear fit between measurements and time
    progressive_drift <- function(x) {
      # Only assess time series with less than 10% missing data in the rolling window
      if(length(x[!is.na(x)]) > (length(x) - (length(x)*0.1))){
        # Fit linear model of measurements vs. time index
        model <- lm(x ~ c(1:length(x)), na.action = na.omit)
        # Extract R-squared value indicating strength of linear trend
        r_squared <- summary(model)$r.squared
        # Return R-squared value
        return(r_squared)
      } else {
        # If too much missing data, set R-squared to 0 (no trend)
        no_slope <- 0
        return(no_slope)
      }
    }
    
    # Function to check if the mean R-squared in a window exceeds threshold
    # Returns TRUE if mean R-squared ≥ 0.60, indicating consistent drift
    check_too_steady <- function(x) {
      mean(x) >= 0.60
    }
    
    # Apply drift detection analysis to the time series
    df <- df %>%
      data.table::data.table() %>%
      # Calculate R-squared values for different window sizes and alignments
      # 96 observations = 1 day at 15-minute intervals
      # 288 observations = 3 days at 15-minute intervals
      dplyr::mutate(
        # 1-day windows with different alignments
        r2_s_right = data.table::frollapply(mean, n = 96, FUN = progressive_drift, align = "right", fill = NA),
        r2_s_center = data.table::frollapply(mean, n = 96, FUN = progressive_drift, align = "left", fill = NA),
        # 3-day windows with different alignments
        r2_l_right = data.table::frollapply(mean, n = 288, FUN = progressive_drift, align = "right", fill = NA),
        r2_l_center = data.table::frollapply(mean, n = 288, FUN = progressive_drift, align = "left", fill = NA),
        # Take the maximum R-squared from any window configuration
        tightest_r = pmax(r2_s_center, r2_s_right, r2_l_center, r2_l_right, na.rm = TRUE),
        # Check if any 1-day period has consistently high R-squared values
        failed = data.table::frollapply(tightest_r, n = 96, FUN = check_too_steady, align = "right", fill = NA)) %>%
      # Add drift flag for periods with consistent linear trends
      # Only add if drift flag doesn't already exist
      add_flag(failed == 1, "drift")
    
    return(df)
  } else {
    # Return unmodified dataframe for non-optical parameters
    return(df)
  }
}