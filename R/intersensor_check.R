#' @title Remove redundant slope violation flags across parameters
#'
#' @description
#' Reduces overflagging by identifying and removing "slope violation" flags that likely 
#' represent real environmental changes rather than sensor malfunctions. When rapid changes 
#' occur simultaneously in multiple parameters, especially in temperature or depth, these 
#' changes typically reflect actual hydrologic events rather than sensor problems.
#'
#' This function processes all parameters from a single site and:
#' 1. Identifies concurrent slope violations across parameters
#' 2. Removes "slope violation" flags from non-temperature, non-depth parameters when they 
#'    coincide with similar flags in temperature or depth
#' 3. Removes all "slope violation" flags from temperature and depth parameters, as these 
#'    parameters rarely exhibit false spikes
#'
#' @param df A data frame containing all parameters for a single site. Must include columns:
#' - `DT_round`: Timestamp for each measurement
#' - `DT_join`: Character timestamp used for joining
#' - `parameter`: Measurement type
#' - `mean`: The calculated mean value of measurements
#' - `flag`: Existing quality flags containing "slope violation" flags to be checked
#'
#' @return A data frame with the same structure as the input, but with the `flag`
#' column updated to remove "slope violation" flags that coincide with temperature 
#' or depth changes, and with all "slope violation" flags removed from temperature 
#' and depth parameters.
#'
#' @examples
#' # Remove redundant slope violation flags at the riverbluffs site
#' riverbluffs_corrected <- intersensor_check(df = all_flagged_data$riverbluffs)
#'
#' # Remove redundant slope violation flags at the boxelder site
#' boxelder_corrected <- intersensor_check(df = all_flagged_data$boxelder)
#'
#' @seealso [add_flag()]

intersensor_check <- function(df){
  # Extract temperature data and flags for the site
  # Include flags from adjacent timestamps for context
  temperature <- df %>%
    data.table::data.table() %>%
    dplyr::select(DT_round, DT_join, parameter, mean, flag) %>%
    dplyr::filter(parameter == "Temperature") %>%
    dplyr::select(DT_join, Temperature = parameter, Temperature_flag = flag) %>%
    dplyr:: mutate(Temperature_front1 = dplyr::lead(Temperature_flag, n = 1),
                   Temperature_back1 = dplyr::lag(Temperature_flag, n = 1))
  
  # Extract depth data and flags for the site
  # Include flags from adjacent timestamps for context
  depth <- df %>%
    data.table::data.table() %>%
    dplyr::select(DT_round, DT_join, parameter, mean, flag) %>%
    dplyr::filter(parameter == "Depth") %>%
    dplyr::select(DT_join, Depth = parameter, Depth_flag = flag) %>%
    dplyr::mutate(Depth_front1 = dplyr::lead(Depth_flag, n = 1),
                  Depth_back1 = dplyr::lag(Depth_flag, n = 1))
  
  # Process non-temperature, non-depth parameters to remove redundant flags
  intersensors_checked <- df %>%
    dplyr::filter(!parameter %in% c("Depth", "Temperature")) %>%
    dplyr::left_join(., temperature, by = "DT_join") %>%
    dplyr::left_join(., depth, by = "DT_join") %>%
    # Identify slope violations that coincide with similar flags in depth or temperature
    # (including timestamps immediately before or after)
    dplyr::mutate(intersensored = dplyr::case_when(grepl("slope violation", flag) &
                                                     (grepl("slope violation", Depth_flag)   | grepl("slope violation", Temperature_flag)   |
                                                        grepl("slope violation", Depth_front1) | grepl("slope violation", Temperature_front1) |
                                                        grepl("slope violation", Depth_back1)  | grepl("slope violation", Temperature_back1)
                                                     ) ~ TRUE)) %>%
    # Remove the "slope violation" flag when it coincides with depth/temperature changes
    dplyr::mutate(flag = ifelse(is.na(intersensored), flag, stringr::str_replace(flag, "slope violation", "")))
  
  # Process temperature and depth parameters
  final_checked_data <- df %>%
    dplyr::filter(parameter %in% c("Depth", "Temperature")) %>%
    # Remove all "slope violation" flags from temperature and depth
    # These parameters rarely exhibit false spikes
    dplyr::mutate(flag = stringr::str_replace(flag, "slope violation", "")) %>%
    # Combine with the processed non-temperature, non-depth parameters
    dplyr::bind_rows(., intersensors_checked) %>%
    # Remove temporary columns added during processing
    dplyr::select(-c(Depth, Depth_flag, Temperature, Temperature_flag))
  
  return(final_checked_data)
}