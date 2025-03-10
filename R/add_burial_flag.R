#' @title Flag all parameters within sonde when sonde burial is detected by DO sensor
#'
#' @description
#' A function designed to identify instances of likely sonde burial by examining
#' dissolved oxygen (DO) sensors for burial indicators, then propagating these flags
#' to all other parameters measured at the same time point. This function works on
#' site-specific dataframes containing multiple parameters.
#'
#' When sediment buries a sonde, the DO sensor often shows distinctive patterns
#' (already flagged as "Possible burial" by upstream functions like `find_do_noise()`).
#' This function ensures all parameters are consistently flagged during burial events.
#'
#' @param df A dataframe containing all parameters for a single site, with a `flag` column.
#' Must include DO parameter readings with pre-existing flags.
#'
#' @return A dataframe with the `flag` column updated to include "Possible burial" flags
#' for all non-DO parameters when the DO sensor indicates burial conditions.
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