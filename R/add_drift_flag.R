#' @title Flag sensor drift in optical measurements
#' @export
#'
#' @description
#' Identifies and flags periods when optical sensors show evidence of progressive drift,
#' which often indicates biofouling issues. The function analyzes time series
#' patterns to detect when measurements show a steady linear trend over time that is
#' unlikely to represent natural environmental conditions.
#'
#' The function only processes optical parameters prone to biofouling:
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
#'@details
#' This function checks for drift in 4 rolling windows and assigns a drift flag if any of these
#' rolling windows has a linear relationship with an R-squared value >= 0.5 for at least 1 day. The four rolling windows are:
#' 1. 1-day window aligned to the right
#' 2. 1-day window centered
#' 3. 3-day window aligned to the right
#' 4. 3-day window centered
#'
#' These windows are calculated using the `slider` package to efficiently compute rolling statistics.
#' The windows are also broken up by site visits when a sensor is cleaned as we know that sensor biofouling is
#' typically reset/stopped from a cleaning visit
#'
#'
#' @examples
#' # Examples are temporarily disabled
#' @seealso [add_flag()]

add_drift_flag <- function(df) {
  
  if (!unique(df$parameter) %in% c( "Turbidity", "Chl-a Fluorescence")) {
    return(df)
  }
  
  param <- unique(df$parameter)[1]
  
  # Rolling window sizes
  win_short <- 96L   # ~1 day
  win_long  <- 288L  # ~3 days
  
  # Progressive drift helper
  progressive_drift <- function(x) {
    #Don't calculate if there are less than 10 points or less than 90% of data is present
    if(length(x) < 10) return(0)
    if (sum(!is.na(x)) < ceiling(0.9 * length(x))) return(0)
    #Compute linear model & extract slope
    model <- lm(x ~ seq_along(x), na.action = na.omit)
    slope <- coef(model)[2]
    
    if (slope <= 0) return(0)
    #extract r^2
    summary(model)$r.squared
  }
  
  # Steady drift check helper (looking for when R2 is consistently high (>0.5))
  too_steady <- function(x) mean(x, na.rm = TRUE) >= 0.5
  # Regex to identify site visit flags
  visit_regex <- "(site\\s*visit|\\bsv\\b)"
  
  new <- df %>%
    #Split data by site visits to avoid crossing cleaning events
    mutate(visit_group = cumsum(str_detect(replace_na(flag, ""), regex(visit_regex, ignore_case = TRUE)))) %>%
    group_by(visit_group)%>%
    # Calculate R-squared values for different window sizes and alignments
    # 1 day = 1440 minutes
    # 3 days = 4320 minutes
    dplyr::mutate(
      # 1-day windows with different alignments
      r2_s_right = slider::slide_index_dbl(.x = mean, .i = DT_round, .f = progressive_drift, .before = minutes(1440)),
      r2_s_center = slider::slide_index_dbl(.x = mean, .i = DT_round, .f = progressive_drift, .before = minutes(720), .after = minutes(720)),
      # 3-day windows with different alignments
      r2_l_right = slider::slide_index_dbl(.x = mean, .i = DT_round, .f = progressive_drift, .before = minutes(4320)),
      r2_l_center = slider::slide_index_dbl(.x = mean, .i = DT_round, .f = progressive_drift, .before = minutes(2160), .after = minutes(2160)),
      # Take the maximum R-squared from any window configuration
      tightest_r = pmax(r2_s_center, r2_s_right, r2_l_center, r2_l_right, na.rm = TRUE),
      # Check if any 1-day period has consistently high R-squared values
      failed = slider::slide_index_dbl(.x = tightest_r, .i = DT_round, .before = minutes(1440), .f = too_steady)
    ) %>%
    data.table::data.table() %>%
    # Add drift flag for periods with consistent linear trends
    add_flag(failed == 1, "drift") %>%
    select(-visit_group, -r2_s_right, -r2_s_center, -r2_l_right, -r2_l_center, -tightest_r, -failed)
  
  
  return(new)
}

