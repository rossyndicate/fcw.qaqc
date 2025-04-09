#' @title Flag dissolved oxygen (DO) sensor noise and potential sonde burial
#' 
#' @description 
#' This function identifies two types of issues with DO sensors:
#' 1. Short-term interference: Flagged when sudden fluctuations occur or DO is 
#' abnormally low.
#' 2. Possible burial: Flagged when persistent interference is detected over a 
#' 24-hour period.
#' This function processes site-parameter dataframes, but only modifies those 
#' containg DO data. For those dataframes without DO data, the function returns
#' the original dataframe unmodified.
#' 
#' @param df A site-parameter dataframe containing water quality measurements. Must include:
#' - `parameter`: Measurement type (function checks for "DO")
#' - `mean`: The calculated mean value of measurements
#' - `back1`: The previous measurement value
#' - `front1`: The next measurement value
#' - `flag`: Existing quality flags (will be updated by this function)
#' 
#' @return A dataframe with the same structure as the input, but with `flag` column
#' updated to include "DO interference" and/or "Possible burial" flags where applicable.
#' 
#' @examples
#' df <- find_do_noise(df = all_data_summary_stats_list$`riverbluffs-DO`)
#' 
#' @seealso [add_flag()]
 
find_do_noise <- function(df){
  
  # Only process dataframes containing dissolved oxygen data
  if("DO" %in% df$parameter){
    
    # Flag short-term DO interference when either:
    # 1. There's a spike in both directions (before and after current reading) of ≥ 0.5 mg/L
    # 2. DO is abnormally low (≤ 5 mg/L)
    df <- df %>%
      add_flag((back1 - mean >= 0.5 & front1 - mean >= 0.5) | (mean <= 5), "DO interference")
    
    
    # Define a function to check if a given window has at least 24 instances of interference
    check_day_hour_window_fail <- function(x) {
      sum(x) >= 24
    }
    
    # Flag possible sensor burial when DO interference persists for 24+ hours
    df <- df %>%
      data.table::data.table() %>%
      # Create binary indicator for DO interference flag
      dplyr::mutate(DO_drift_binary = ifelse(grepl("DO interference", flag), 1, 0)) %>%
      # Apply rolling window check in both right-aligned and center-aligned modes
      dplyr::mutate(right = data.table::frollapply(DO_drift_binary, n = 96, FUN = check_day_hour_window_fail, align = "right", fill = NA),
                    center = data.table::frollapply(DO_drift_binary, n = 96, FUN = check_day_hour_window_fail, align = "center", fill = NA)) %>%
      # Flag possible burial if either window check indicates persistent interference
      add_flag(right == 1 | center == 1, "Possible burial")
    
    
    return(df)
    
  } else {
    
    # Return unmodified dataframe for non-DO parameters
    return(df)
    
  }
  
}
