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
#' # Examples are temporarily disabled
#' @seealso [add_flag()]

add_drift_flag <- function(df) {
  
  if (!unique(df$parameter) %in% c("FDOM Fluorescence", "Turbidity", "Chl-a Fluorescence")) {
    return(df)
  }
  
  param <- unique(df$parameter)[1]
  
  # Rolling window sizes
  win_short <- 96L   # ~1 day
  win_long  <- 288L  # ~3 days
  
  # Progressive drift helper
  progressive_drift <- function(x) {
    if (sum(!is.na(x)) < ceiling(0.9 * length(x))) return(0)
    
    model <- lm(x ~ seq_along(x), na.action = na.omit)
    slope <- coef(model)[2]
    
    if (param != "FDOM Fluorescence" && slope <= 0) return(0)
    if (param == "FDOM Fluorescence" && slope >  0) return(0)
    
    summary(model)$r.squared
  }
  
  # Steady drift check
  too_steady <- function(x) mean(x, na.rm = TRUE) >= 0.60
  
  visit_regex <- "(site\\s*visit|\\bsv\\b)"
  
  df %>%
    mutate(visit_group = cumsum(str_detect(replace_na(flag, ""), regex(visit_regex, ignore_case = TRUE)))) %>%
    group_by(visit_group) %>%
    mutate(r2_s_right = rollapply(mean, width = win_short, FUN = progressive_drift, align = "right",  fill = NA),
           r2_l_right = rollapply(mean, width = win_long,  FUN = progressive_drift, align = "right",  fill = NA),
           r2_s_center = rollapply(mean, width = win_short, FUN = progressive_drift, align = "center", fill = NA),
           r2_l_center = rollapply(mean, width = win_long,  FUN = progressive_drift, align = "center", fill = NA),
           tightest_r = pmax(r2_s_center, r2_s_right, r2_l_center, r2_l_right, na.rm = TRUE),
           failed = rollapply(tightest_r, width = win_short, FUN = too_steady, align = "right", fill = NA)) %>%
    ungroup() %>%
    add_flag(failed, "drift") %>%
    select(-visit_group, -r2_s_right, -r2_s_center, -r2_l_right, -r2_l_center, -tightest_r, -failed)