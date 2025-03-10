#' @title Get the start dates for the HydroVu API pull from the historically
#' flagged data
#'
#' @description
#' This function finds the most recent timestamp (max datetime) in the Temperature 
#' parameter data frames for each monitoring site's historically flagged data. 
#' Temperature is used as the reference parameter because it is consistently 
#' tracked by all sondes and therefore provides the most reliable indication of 
#' when data was last collected from each site.
#' 
#' The resulting timestamps serve as starting points for new API data requests,
#' ensuring continuous data collection without gaps or unnecessary duplication.
#'
#' @param incoming_historically_flagged_data_list A list of dataframes containing 
#' historical water quality data that has already been processed through the QAQC 
#' workflow. Each list element should represent a site-parameter combination and 
#' be named with the "site-parameter" naming convention.
#'
#' @return A dataframe containing two columns:
#' - site: The monitoring location identifier
#' - DT_round: The timestamp (in MST timezone) of the most recent Temperature 
#' reading for each site, which will be used as the start date for new API pulls
#'
#' @examples
#' # Get start dates from historical data
#' start_dates <- get_start_dates(incoming_historically_flagged_data_list = historical_data)
#'
#' @seealso [api_puller()]
#' @seealso [munge_api_data()]

get_start_dates <- function(incoming_historically_flagged_data_list) {
  
  # Extract only the Temperature parameter dataframes from the historical data list
  temperature_subset <- grep("Temperature", names(incoming_historically_flagged_data_list))
  
  # For each site's Temperature dataframe, extract the most recent timestamp
  start_dates_df <- purrr::map_dfr(incoming_historically_flagged_data_list[temperature_subset],
                                  function(temperature_df){
                                    temperature_df %>%
                                      # Select only the timestamp and site columns
                                      dplyr::select(DT_round, site) %>%
                                      # Filter to keep only the row with the maximum (most recent) timestamp
                                      dplyr::filter(DT_round == max(DT_round))
                                  })
  
  return(start_dates_df)
}