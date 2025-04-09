#' @title Flag measurements when sonde was not fully submerged
#'
#' @description
#' Identifies and flags water quality measurements collected when the monitoring
#' sonde was not fully submerged in water. This function uses depth sensor readings
#' to determine submersion status and applies flags across all parameters measured
#' at the same time. The data will be flagged if the value in the `relative_depth`
#' column is less than or equal to 0.
#'
#' @param df A dataframe containing all parameters for a single site. Must include columns:
#' - `parameter`: Measurement type (function requires "Depth" parameter to be present)
#' - `DT_join`: Character timestamp used for joining
#' - `mean`: The calculated mean value of measurements
#' - `flag`: Existing quality flags (will be updated by this function)
#'
#' @return A dataframe with the same structure as the input, but with the `flag`
#' column updated to include "sonde unsubmerged" for all parameters when depth
#' readings indicate the sonde was not fully underwater.
#'
#' @examples
#' # Flag unsubmerged periods at the riverbluffs site
#' riverbluffs_flagged <- add_unsubmerged_flag(df = all_data_flagged$riverbluffs)
#'
#' # Flag unsubmerged periods at the boxelder site
#' boxelder_flagged <- add_unsubmerged_flag(df = all_data_flagged$boxelder)
#'
#' @seealso [add_flag()]

add_unsubmerged_flag <- function(df){
  # Extract depth measurements for the site
  depth <- df %>%
    data.table::data.table() %>%
    dplyr::select(DT_round, DT_join, parameter, mean) %>%
    dplyr::filter(parameter == "Depth") %>%
    dplyr::select(DT_join, Depth = mean)
  
  # Join depth measurements with all parameters and apply flag
  depth_checked <- df %>%
    dplyr::left_join(., depth, by = "DT_join") %>%
    # Flag all parameters when depth is 0 or negative
    add_flag(., Depth <= 0, "sonde unsubmerged") %>%
    # Remove temporary depth column to maintain original structure
    dplyr::select(-Depth)
  
  return(depth_checked)
}