#' @title Flag measurements taken during freezing conditions
#'
#' @description
#' Identifies and flags water quality measurements collected when water temperature
#' was at or below freezing (0°C). This function helps identify periods when ice formation
#' may affect sensor readings across all parameters at a site.
#'
#' Unlike most flagging functions that operate on individual parameters, this function
#' requires a site-level dataframe containing temperature data and applies freezing
#' flags across all parameters.
#'
#' @param df A data frame containing all parameters for a single site. Must include columns:
#' - `DT_round`: Timestamp for each measurement
#' - `DT_join`: Character timestamp used for joining
#' - `parameter`: Measurement type (function looks for "Temperature")
#' - `mean`: The calculated mean value of measurements
#' - `flag`: Existing quality flags (will be updated by this function)
#'
#' @return A data frame with the same structure as the input, but with the `flag`
#' column updated to include "frozen" for all parameters when water temperature
#' is at or below 0°C.
#'
#' @examples
#' # Flag freezing conditions at the archery site
#' archery_flagged <- add_frozen_flag(df = all_data_flagged$archery)
#'
#' # Flag freezing conditions at the boxelder site
#' boxelder_flagged <- add_frozen_flag(df = all_data_flagged$boxelder)
#'
#' @seealso [add_flag()]
add_frozen_flag <- function(df){
  # Extract temperature data for the site
  temperature <- df %>%
    data.table::data.table() %>%
    dplyr::select(DT_round, DT_join, parameter, mean) %>%
    dplyr::filter(parameter == "Temperature") %>%
    dplyr::select(DT_join, Temperature = mean)
  
  # Join temperature data to all parameters and apply freezing flag
  temperature_checked <- df %>%
    dplyr::left_join(., temperature, by = "DT_join") %>%
    # Flag all parameters when water temperature is at or below freezing
    add_flag(., Temperature <= 0, "frozen") %>%
    # Remove the temporary temperature column to maintain original structure
    dplyr::select(-Temperature)
  
  return(temperature_checked)
}