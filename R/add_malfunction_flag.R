#' @title Flag known sensor malfunctions from field notes
#'
#' @description
#' Adds malfunction flags to water quality data based on field technician observations
#' and notes. This function identifies periods when sensors were known to be malfunctioning
#' and categorizes these into specific types of malfunctions.
#'
#' The function uses a separate `mal_flag` column rather than the standard `flag` column,
#' allowing malfunction information to be treated differently in downstream processing.
#'
#' @param df A data frame containing water quality measurements. Must include columns:
#' - `site`: Standardized site name
#' - `parameter`: The measurement type
#' - `DT_round`: Timestamp for each measurement
#' - `flag`: Existing quality flags
#'
#' @param malfunction_records A data frame containing records of known sensor malfunctions 
#' with columns:
#' - `site`: Site name matching the df site column
#' - `parameter`: Parameter name (NA indicates the entire sonde was malfunctioning)
#' - `start_DT`: Start timestamp of the malfunction period
#' - `end_DT`: End timestamp of the malfunction period (NA indicates ongoing)
#' - `notes`: Text description of the malfunction
#'
#' @return A data frame with the same structure as the input, plus a `mal_flag` column
#' containing descriptions of malfunction types for affected measurements.
#'
#' @examples
#' # Flag known malfunctions in dissolved oxygen data
#' riverbluffs_do_flagged <- add_malfunction_flag(
#'   df = intersensor_flags$`riverbluffs-DO`,
#'   malfunction_records = sensor_malfunction_notes
#' )
#'
#' @seealso [grab_mWater_malfunction_notes()]
add_malfunction_flag <- function(df, malfunction_records){
  
  # Extract unique site and parameter from input dataframe
  df_site <- unique(df$site)
  df_parameter <- unique(df$parameter)
  
  # Create mal_flag column if it doesn't exist
  df <- df %>%
    dplyr::mutate(mal_flag = ifelse("mal_flag" %in% names(.), mal_flag, NA))
  
  # Helper function to add malfunction flags while preserving existing ones
  add_mal_flag <- function(df, condition_arg, description_arg) {
    df <- df %>% dplyr::mutate(mal_flag = dplyr::case_when(
      {{condition_arg}} ~ if_else(is.na(mal_flag), paste(description_arg),
                                  ifelse(!grepl(description_arg, mal_flag), paste(mal_flag, description_arg, sep = ";\n"), mal_flag)),
      TRUE ~ mal_flag))
    return(df)
  }
  
  # Filter malfunction records relevant to this site and parameter
  malfunction_records_filtered <- malfunction_records %>%
    dplyr::filter(site == df_site) %>%
    # Include records for this parameter, sonde-wide issues, and burial/submersion issues
    dplyr::filter(is.na(parameter) | parameter == df_parameter | 
                    grepl("buried|burial|bury|sediment|roots", notes, ignore.case = TRUE) | 
                    grepl("not submerged", notes, ignore.case = TRUE)) %>%
    # Handle ongoing malfunctions (no end date)
    dplyr::mutate(end_DT = ifelse(is.na(end_DT), lubridate::ymd_hms("9999-12-31 23:59:59", tz = "MST"), end_DT)) %>%
    dplyr::mutate(end_DT = as.POSIXct(end_DT, tz = "MST")) %>%
    tibble::rowid_to_column()
  
  # Special handling for ORP parameter, which is affected by pH sensor issues
  if(df_parameter == "ORP"){
    malfunction_records_filtered <- malfunction_records %>%
      dplyr::filter(site == df_site) %>%
      # For ORP, also include pH issues
      dplyr::filter(is.na(parameter) | parameter == df_parameter | parameter == "pH" |
                      grepl("buried|burial|bury|sediment|roots", notes, ignore.case = TRUE) | 
                      grepl("not submerged", notes, ignore.case = TRUE)) %>%
      dplyr::mutate(end_DT = ifelse(is.na(end_DT), lubridate::ymd_hms("9999-12-31 23:59:59", tz = "MST"), end_DT)) %>%
      dplyr::mutate(end_DT = as.POSIXct(end_DT, tz = "MST")) %>%
      tibble::rowid_to_column()
  }
  
  # Only process if there are relevant malfunction records
  if (nrow(malfunction_records_filtered > 0)) {
    
    # Categorize malfunctions into specific types based on notes
    
    # Sensor drift and biofouling
    drift <- malfunction_records_filtered %>%
      dplyr::filter(grepl("grime|gunk|drift|biofoul|biofilm", notes, ignore.case = TRUE))
    
    # Sensor burial
    burial <- malfunction_records_filtered %>%
      dplyr::filter(grepl("buried|burial|bury|sediment|roots", notes, ignore.case = TRUE))
    
    # Depth calibration issues
    depth_funk <- malfunction_records_filtered %>%
      dplyr::filter(grepl("improper level calibration", notes, ignore.case = TRUE))
    
    # Sensors not in water
    unsubmerged <- malfunction_records_filtered %>%
      dplyr::filter(grepl("not submerged", notes, ignore.case = TRUE))
    
    # Any other sensor malfunctions
    general_malfunction <- malfunction_records_filtered %>%
      dplyr::filter(!rowid %in% drift$rowid & !rowid %in% burial$rowid &
                      !rowid %in% depth_funk$rowid & !rowid %in% unsubmerged$rowid)
    
    # Create time intervals for each malfunction type
    drift_interval_list <- map2(
      .x = drift$start_DT,
      .y = drift$end_DT,
      .f = ~lubridate::interval(.x, .y, tz = "MST"))
    
    burial_interval_list <- map2(
      .x = burial$start_DT,
      .y = burial$end_DT,
      .f = ~lubridate::interval(.x, .y, tz = "MST"))
    
    depth_interval_list <- map2(
      .x = depth_funk$start_DT,
      .y = depth_funk$end_DT,
      .f = ~lubridate::interval(.x, .y, tz = "MST"))
    
    unsubmerged_interval_list <- map2(
      .x = unsubmerged$start_DT,
      .y = unsubmerged$end_DT,
      .f = ~lubridate::interval(.x, .y, tz = "MST"))
    
    general_interval_list <- map2(
      .x = general_malfunction$start_DT,
      .y = general_malfunction$end_DT,
      .f = ~lubridate::interval(.x, .y, tz = "MST"))
    
    # Apply specific flags for each malfunction type
    try(df <- df %>%
          add_mal_flag(DT_round %within% burial_interval_list, "reported sonde burial"))
    
    try(df <- df %>%
          add_mal_flag(DT_round %within% drift_interval_list, "reported sensor biofouling"))
    
    try(df <- df %>%
          add_mal_flag(DT_round %within% depth_interval_list, "reported depth calibration malfunction"))
    
    try(df <- df %>%
          add_mal_flag(DT_round %within% unsubmerged_interval_list, "reported sonde unsubmerged"))
    
    try(df <- df %>%
          add_mal_flag(DT_round %within% general_interval_list, "reported sensor malfunction"))
  }
  
  return(df)
}