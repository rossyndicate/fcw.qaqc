#' @title Get the start dates for the HydroVu API pull from the historically
#' flagged data
#'
#' @description
#' This function finds the max datetime in the temperature data
#' frames for each site's historically flagged data. We use temperature because
#' it is always being tracked in the sensors and thus is the most accurate
#' representation of the last API pull for a given sensor.
#'
#' @param incoming_historically_flagged_data_list This is the output from
#' `flagged_data_dfs` and is the historically flagged data.
#'
#' @returns
#' A data frame that has the sites and their corresponding starting
#' date-times' for the HydroVu API pull. This data frame will then get passed into
#' `incoming_data_csvs_upload`.

get_start_dates <- function(incoming_historically_flagged_data_list) {
  
  temperature_subset <- grep("Temperature", names(incoming_historically_flagged_data_list))
  
  start_dates_df <- purrr::map_dfr(incoming_historically_flagged_data_list[temperature_subset],
                                   function(temperature_df){
                                     temperature_df %>%
                                       dplyr::select(DT_round, site) %>%
                                       dplyr::filter(DT_round == max(DT_round))
                                   })
  
  return(start_dates_df)
  
}
