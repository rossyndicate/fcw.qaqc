#' @title Flag periods when sonde depth was changed
#'
#' @description
#' Identifies and flags time periods when sondes were physically 
#' moved or repositioned in their housings. Sonde depth changes can affect multiple 
#' parameters and cause discontinuities in the data that aren't related to actual 
#' environmental changes.
#'
#' The function handles two different data processing workflows:
#' - Pre-2024 data: Uses a separate table that tracks when sondes were moved
#' - Post-2024 data: Uses information already integrated into field notes 
#'   through the `sonde_moved` boolean field
#'
#' @param df A data frame containing water quality measurements. Must include columns:
#' - `site`: Standardized site name
#' - `DT_join`: Character timestamp in a format compatible with joining to field notes
#' - `flag`: Existing quality flags (will be updated by this function)
#' - `sonde_moved`: Boolean field indicating if sonde was moved (only required when `post2024 = TRUE`)
#'
#' @param level_shift_table A data frame containing field notes with information about 
#' when sondes were physically moved. When `post2024 = FALSE`, must include columns:
#' - `site`: Site name matching the df site column
#' - `DT_join`: Timestamp of the sonde movement
#' - `type`: Field identifying records where type = "sonde moved"
#'
#' @param post2024 Logical value indicating whether the data is from before (FALSE) or 
#' after (TRUE) the 2024 field season. Default is TRUE.
#'
#' @return A data frame with the same structure as the input, plus an additional
#' `depth_change` column containing "sonde moved" flags where applicable.
#'
#' @examples
#' # Flag depth changes in post-2024 data
#' riverbluffs_do_flagged <- add_depth_shift_flag(
#'   df = combined_data$`riverbluffs-DO`, 
#'   level_shift_table = all_field_notes
#' )
#'
#' # Flag depth changes in pre-2024 historical data
#' historical_do_flagged <- add_depth_shift_flag(
#'   df = historical_data$`riverbluffs-DO`,
#'   level_shift_table = depth_shift_records,
#'   post2024 = FALSE
#' )
#'
#' @seealso [add_field_flag()]
#' @seealso [add_flag()]

add_depth_shift_flag <- function(df, level_shift_table, post2024 = TRUE){
  if(post2024 == FALSE){
    # For pre-2024 data, extract sonde movement events from the shift table
    # and join them with the measurement data
    depth_shifts <- level_shift_table %>%
      dplyr::filter(type == "sonde moved") %>%
      dplyr::mutate(DT_join = as.character(lubridate::ymd_hms(DT_join)))
    
    add_shifts <- df %>%
      dplyr::left_join(., depth_shifts, by = c("site", "DT_join")) %>%
      dplyr::mutate(depth_change = ifelse(!is.na(type), "sonde moved", NA)) %>%
      dplyr::select(-type)
    
    return(add_shifts)
  } else {
    # For post-2024 data, the sonde_moved field is already in the dataset
    # from the integrated field notes
    df <- df %>%
      dplyr::mutate(depth_change = ifelse(sonde_moved == TRUE, "sonde moved", NA))
    
    return(df)
    
  }
}