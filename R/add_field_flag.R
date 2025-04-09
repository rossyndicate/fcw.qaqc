#' @title Flag data periods affected by field activities
#'
#' @description
#' Identifies and flags time periods in water quality data that are affected by
#' field activities such as sensor maintenance, calibration, or deployment changes.
#' This function marks three distinct conditions related to field operations:
#'
#' 1. "sonde not employed" - Periods when the sensor was physically removed from
#'    the water body (indicated by sonde_employed = 1 in field notes)
#'
#' 2. "site visit" - Exact timestamps when field technicians were actively
#'    working with the equipment on site
#'
#' 3. "sv window" - A buffer period around site visits (15 minutes before and
#'    60 minutes after) when data may be affected by field activities
#'
#' These flags identify periods when readings may not reflect natural environmental 
#' conditions due to human interference. The function requires that field notes 
#' have already been joined to the water quality data.
#'
#' @param df A dataframe containing water quality data with field notes already
#' joined. Must include columns:
#' - flag: Existing quality flags
#' - sonde_employed: Binary indicator of sonde deployment status (1 = not in water)
#' - last_site_visit: Timestamp of the most recent site visit
#' - DT_round: Rounded timestamp for each data record
#'
#' @return A dataframe with the same structure as the input, but with the flag
#' column updated to include "sonde not employed", "site visit", and/or "sv window"
#' flags as appropriate.
#'
#' @examples
#' # Flag field activities in dissolved oxygen data for riverbluffs site
#' riverbluffs_do_flagged <- add_field_flag(df = combined_data$`riverbluffs-DO`)
#'
#' # Flag field activities in temperature data for boxelder site
#' boxelder_temp_flagged <- add_field_flag(df = combined_data$`boxelder-Temperature`)
#'
#' @seealso [add_field_notes()]
#' @seealso [add_flag()]

add_field_flag <- function(df) {
  
  # First, flag periods when the sonde was physically removed from the water
  # This identifies data that doesn't represent in-situ water conditions
  df <- df %>%
    add_flag(sonde_employed == 1, "sonde not employed") %>%
    
    # Next, flag the exact timestamps when technicians were at the site
    # These represent direct human interference with the equipment
    add_flag(as.character(last_site_visit) == as.character(DT_round), "site visit")
  
  # Add flags for the post-visit window (60 minutes after a site visit)
  # This accounts for the recovery period after equipment handling
  # For 15-minute data, this means checking the next 4 timestamps after a site visit
  for (i in 1:4) {
    df <- df %>%
      # Look back i steps to see if there was a site visit flag
      # If found, mark this timestamp as within the recovery window
      add_flag(lag(stringr::str_detect(flag, "site visit"), n = i), "sv window")
  }
  
  # Add flags for the pre-visit window (15 minutes before a site visit)
  # This accounts for potential disturbance before logging the visit time
  df <- df %>%
    # Look ahead 1 step to see if there will be a site visit flag
    # If found, mark this timestamp as within the preparation window
    add_flag(lead(stringr::str_detect(flag, "site visit"), n = 1), "sv window")
  
  return(df)
}