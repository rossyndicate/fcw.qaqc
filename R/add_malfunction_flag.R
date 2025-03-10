#' @title Add malfunction flags to a data frame based on field notes and HydroVu.
#'
#' @description
#' This function adds the 'sensor malfunction' flag to a data frame by the dates,
#' sites, and parameters that members of the lab know contain erroneous data due
#' to sensor malfunctions. Note that this flag is used in two instances: first
#' when removing erroneous data from our statistics calculations and again during
#' the actual flagging step in the QAQC pipeline.
#' @param df A data frame with a `flag` column.
#' @param malfunction_records The malfunction records pulled from mWater
#' @return A data frame with a `flag` column that has been updated with the
#' 'sensor malfunction' flag.
#' @seealso [grab_mWater_malfunction_notes()]

add_malfunction_flag <- function(df, malfunction_records){

  # Filter malfunction records for relevant information
  df_site <- unique(df$site)
  df_parameter <- unique(df$parameter)

  df <- df %>%
    dplyr::mutate(mal_flag = ifelse("mal_flag" %in% names(.), mal_flag, NA))
  
  add_mal_flag <- function(df, condition_arg, description_arg) {
    df <- df %>% dplyr::mutate(mal_flag = dplyr::case_when(
      {{condition_arg}} ~ if_else(is.na(mal_flag), paste(description_arg),
                                  ifelse(!grepl(description_arg, mal_flag), paste(mal_flag, description_arg, sep = ";\n"), mal_flag)),
      TRUE ~ mal_flag))
    return(df)
  }
  
  malfunction_records_filtered <- malfunction_records %>%
    dplyr::filter(site == df_site) %>%
    # if parameter is NA, that means entire sonde was malfunctioning. Otherwise,
    # just the parameter listed:
    dplyr::filter(is.na(parameter) | parameter == df_parameter | 
                  grepl("buried|burial|bury|sediment|roots", notes, ignore.case = TRUE) | 
                  grepl("not submerged", notes, ignore.case = TRUE)) %>%
    dplyr::mutate(end_DT = ifelse(is.na(end_DT), lubridate::ymd_hms("9999-12-31 23:59:59", tz = "MST"), end_DT)) %>%
    dplyr::mutate(end_DT = as.POSIXct(end_DT, tz = "MST")) %>%
    tibble::rowid_to_column()

  if(df_parameter == "ORP"){
  malfunction_records_filtered <- malfunction_records %>%
    dplyr::filter(site == df_site) %>%
    # if parameter is NA, that means entire sonde was malfunctioning. Otherwise,
    # just the parameter listed:
    dplyr::filter(is.na(parameter) | parameter == df_parameter | parameter == "pH" |
                    grepl("buried|burial|bury|sediment|roots", notes, ignore.case = TRUE) | 
                    grepl("not submerged", notes, ignore.case = TRUE)) %>%
    dplyr::mutate(end_DT = ifelse(is.na(end_DT), lubridate::ymd_hms("9999-12-31 23:59:59", tz = "MST"), end_DT)) %>%
    dplyr::mutate(end_DT = as.POSIXct(end_DT, tz = "MST")) %>%
    tibble::rowid_to_column()
  }

  if (nrow(malfunction_records_filtered > 0)) {

    # Identify known (field crew input) instances of...

    # Sensor drift
    drift <- malfunction_records_filtered %>%
      dplyr::filter(grepl("grime|gunk|drift|biofoul|biofilm", notes, ignore.case = TRUE))

    # Sensor burial
    burial <- malfunction_records_filtered %>%
      dplyr::filter(grepl("buried|burial|bury|sediment|roots", notes, ignore.case = TRUE))

    # Bad level calibrations (is now built-in to `fix_calibration.R`, SHOULD BE redundant flag)
    depth_funk <- malfunction_records_filtered %>%
      dplyr::filter(grepl("improper level calibration", notes, ignore.case = TRUE))

    # Sensors not in water:
    unsubmerged <- malfunction_records_filtered %>%
      dplyr::filter(grepl("not submerged", notes, ignore.case = TRUE))

    # All other sensor malfunctions:
    general_malfunction <- malfunction_records_filtered %>%
      dplyr::filter(!rowid %in% drift$rowid & !rowid %in% burial$rowid &
               !rowid %in% depth_funk$rowid & !rowid %in% unsubmerged$rowid)

    # make a list of date intervals where sensor was malfunctioning
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

    # If DT_round is within any of the intervals where a sensor was malfunctioning, flag it
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
