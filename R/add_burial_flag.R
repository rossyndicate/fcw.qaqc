#' @title Flag all parameters within sonde when sonde burial is detected by DO sensor
#'
#' @description
#' Identifies and flags all sensor parameters when evidence of sonde burial is detected
#' by the dissolved oxygen (DO) sensor. When a monitoring sonde becomes buried in sediment,
#' all measurements are affected, but the DO sensor often provides the clearest indication
#' of burial events.
#'
#' This function propagates burial flags detected by the `find_do_noise()` function to all
#' other parameters measured at the same time points. This ensures consistent flagging
#' across all measurements during burial events.
#'
#' Unlike most flagging functions, this function operates on site-level dataframes
#' containing multiple parameters rather than individual site-parameter combinations.
#'
#' @param df A dataframe containing all parameters for a single site. Must include columns:
#' - `parameter`: Measurement type (function looks for "DO" parameter)
#' - `DT_join`: Character timestamp used for joining measurements across parameters
#' - `flag`: Existing quality flags, with "Possible burial" flags already applied to DO
#'   measurements by the `find_do_noise()` function
#'
#' @return A dataframe with the same structure as the input, but with the `flag`
#' column updated to include "Possible burial" flags for all parameters when burial
#' is detected by the DO sensor.
#'
#' @examples
#' add_burial_flag(df = site_data$riverbluffs)
#' add_burial_flag(df = site_data$boxelder)
#'
#' @seealso [add_flag()]
#' @seealso [find_do_noise()]

add_burial_flag <- function(df){  
  # Extract DO readings and their flag status for the site
  # This creates a reference table of timestamps where burial was detected
  do <- df %>%
    data.table::data.table() %>%
    dplyr::filter(parameter == "DO") %>%
    dplyr::select(DT_join, do_flag = flag)
  
  
  do_checked <- df %>%
    # Process all parameters except DO first to avoid duplicate flagging
    dplyr::filter(!parameter %in% c("DO")) %>%
    # Join with DO flags using timestamp to align readings across parameters
    dplyr::left_join(., do, by = "DT_join") %>%
    # Add burial flag to all parameters at timestamps where DO indicates burial
    add_flag(grepl("Possible burial", do_flag), "Possible burial") %>%
    # Remove the temporary DO flag column we added
    dplyr::select(-do_flag) %>%
    # Add back the original DO parameter data (already properly flagged)
    dplyr::bind_rows(df %>% filter(parameter == "DO")) %>%
    # Remove any duplicate rows that might have been created
    dplyr::distinct()
    
  return(do_checked)
  
}