# "Placeholder" function for determining when sonde was moved in its housing. Will integrate this information into field notes
#  so that this csv doesn't need to exist in the future.
#' @title Add a depth shift flag
#'
#' @description A function for determining when sonde was moved in its housing
#' @param df A data frame with a `flag` column.
#' @param level_shift_table The data frame containing information about when the sonde was shifted.
#' @param post2024 Whether the data in the QAQC pipeline is before (FALSE) or after (TRUE) the 2024 field season.
#' @return A data frame with a `flag` column that has been updated with the "sonde moved" flag.
#'
add_depth_shift_flag <- function(df, level_shift_table, post2024 = TRUE){
if(post2024 == FALSE){
  depth_shifts <- level_shift_table %>%
    dplyr::filter(type == "sonde moved") %>%
    dplyr::mutate(DT_join = as.character(lubridate::ymd_hms(DT_join)))

  add_shifts <- df %>%
    dplyr::left_join(., depth_shifts, by = c("site", "DT_join")) %>%
    dplyr::mutate(depth_change = ifelse(!is.na(type), "sonde moved", NA)) %>%
    dplyr::select(-type)

  return(add_shifts)

} else {

  
  df <- df %>%
    dplyr::mutate(depth_change = ifelse(sonde_moved == TRUE, "sonde moved", NA))
  
  return(df)
  
}
}

